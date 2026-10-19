package thredder;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.zip.CRC32;
import static java.nio.file.StandardOpenOption.*;

/** Single-owner, append-only, forced full-record journal. Corrupt/torn data fails closed. */
final class Journal implements AutoCloseable {
    static final int MAGIC = 0x54485234;
    static final int MAX_RECORD = 1_048_576;
    record Record(UUID id, String name, JobDescriptor descriptor, int priority, Scheduler.State state,
                  Instant submitted, Instant started, Instant finished, String error) { }
    // Package-only seam for process-crash tests; production always uses NONE.
    interface Probe {
        void at(String stage, Record record);
        Probe NONE = (stage, record) -> { };
    }
    private final FileChannel channel;
    private final FileLock lock;
    private final Probe probe;
    private boolean poisoned;
    private boolean closed;

    Journal(Path path, Probe probe) {
        this.probe = probe;
        FileChannel opened = null;
        FileLock held = null;
        try {
            Path absolute = path.toAbsolutePath();
            Path parent = absolute.getParent();
            if (!Files.isDirectory(parent)) throw new IOException("Store parent directory must already exist: " + parent);
            opened = FileChannel.open(absolute, CREATE, READ, WRITE);
            held = opened.tryLock();
            if (held == null) throw new IOException("Store is already in use: " + path);
            // Persist the directory entry too; unsupported storage fails rather than promising durability.
            try (FileChannel directory = FileChannel.open(parent, READ)) { directory.force(true); }
            channel = opened;
            lock = held;
        } catch (IOException | RuntimeException failure) {
            if (held != null) try { held.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            if (opened != null) try { opened.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            if (failure instanceof IOException io) throw new UncheckedIOException(io);
            throw (RuntimeException) failure;
        }
    }

    List<Record> recover() {
        try {
            Map<UUID, Record> latest = new LinkedHashMap<>();
            channel.position(0);
            long offset = 0;
            while (offset < channel.size()) {
                if (channel.size() - offset < 12) throw corrupt(offset, "incomplete frame header");
                ByteBuffer header = ByteBuffer.allocate(12);
                readFully(header); header.flip();
                int magic = header.getInt(), size = header.getInt(), expected = header.getInt();
                if (magic != MAGIC || size < 1 || size > MAX_RECORD) throw corrupt(offset, "invalid version or frame length");
                if (channel.size() - channel.position() < size) throw corrupt(offset, "incomplete record");
                ByteBuffer body = ByteBuffer.allocate(size); readFully(body);
                CRC32 crc = new CRC32(); crc.update(body.array());
                if ((int) crc.getValue() != expected) throw corrupt(offset, "checksum mismatch");
                Record record = decode(body.array());
                validateHistory(latest.get(record.id()), record);
                latest.put(record.id(), record);
                offset = channel.position();
            }
            return List.copyOf(latest.values());
        } catch (IOException failure) { throw new UncheckedIOException(failure); }
        catch (RuntimeException failure) { throw new IllegalStateException("Invalid journal data; store was not modified", failure); }
    }
    private static IOException corrupt(long offset, String message) {
        return new IOException("Invalid journal at byte " + offset + ": " + message + "; preserve the file for recovery, do not silently retry");
    }
    private void readFully(ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) if (channel.read(buffer) < 0) throw new EOFException("Truncated journal");
    }
    void append(Record record) {
        if (poisoned || closed) throw new IllegalStateException("Journal is unavailable");
        // Encode before touching the channel so invalid requests do not poison the store.
        byte[] bytes = encode(record);
        try {
            probe.at("before-write", record);
            CRC32 crc = new CRC32(); crc.update(bytes);
            ByteBuffer frame = ByteBuffer.allocate(12 + bytes.length);
            frame.putInt(MAGIC).putInt(bytes.length).putInt((int) crc.getValue()).put(bytes).flip();
            channel.position(channel.size());
            while (frame.hasRemaining()) channel.write(frame);
            channel.force(true);
            probe.at("after-force", record);
        } catch (IOException failure) {
            poisoned = true;
            throw new UncheckedIOException(failure);
        } catch (RuntimeException failure) {
            poisoned = true;
            throw failure;
        }
    }
    private static byte[] encode(Record record) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                out.writeUTF(record.id().toString());
                out.writeUTF(record.name());
                out.writeUTF(record.descriptor().type());
                out.writeUTF(record.descriptor().argument());
                out.writeInt(record.priority()); out.writeUTF(record.state().name());
                out.writeUTF(record.submitted().toString());
                out.writeUTF(record.started() == null ? "" : record.started().toString());
                out.writeUTF(record.finished() == null ? "" : record.finished().toString());
                out.writeBoolean(record.error() != null);
                if (record.error() != null) out.writeUTF(record.error());
            }
            if (bytes.size() > MAX_RECORD) throw new IllegalArgumentException("Job record is too large");
            return bytes.toByteArray();
        } catch (IOException failure) { throw new IllegalArgumentException("Job text exceeds journal field limits", failure); }
    }
    static void validateRecordSize(Record record) { encode(record); }
    private static Record decode(byte[] bytes) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            UUID id = UUID.fromString(in.readUTF()); String name = in.readUTF();
            JobDescriptor descriptor = new JobDescriptor(in.readUTF(), in.readUTF());
            int priority = in.readInt(); Scheduler.State state = Scheduler.State.valueOf(in.readUTF());
            Instant submitted = Instant.parse(in.readUTF());
            String startedText = in.readUTF(), finishedText = in.readUTF();
            Instant started = startedText.isEmpty() ? null : Instant.parse(startedText);
            Instant finished = finishedText.isEmpty() ? null : Instant.parse(finishedText);
            String error = in.readBoolean() ? in.readUTF() : null;
            if (in.available() != 0 || name.isBlank() || priority < 0 || priority > 10) throw new IOException("Invalid record fields");
            if (state == Scheduler.State.QUEUED && (started != null || finished != null)) throw new IOException("Invalid queued record");
            if ((state == Scheduler.State.RUNNING || state == Scheduler.State.UNRESOLVED) && (started == null || finished != null)) throw new IOException("Invalid running record");
            if ((state == Scheduler.State.SUCCEEDED || state == Scheduler.State.FAILED || state == Scheduler.State.CANCELLED) && finished == null) throw new IOException("Missing completion timestamp");
            return new Record(id, name, descriptor, priority, state, submitted, started, finished, error);
        }
    }
    private static void validateHistory(Record old, Record next) throws IOException {
        if (old == null) {
            if (next.state() != Scheduler.State.QUEUED) throw new IOException("First record must be QUEUED");
            return;
        }
        if (!old.name().equals(next.name()) || !old.descriptor().equals(next.descriptor())
                || old.priority() != next.priority() || !old.submitted().equals(next.submitted())) throw new IOException("Job identity changed");
        boolean legal = switch (old.state()) {
            case QUEUED -> next.state() == Scheduler.State.RUNNING || next.state() == Scheduler.State.CANCELLED;
            case RUNNING -> next.state() != Scheduler.State.QUEUED && next.state() != Scheduler.State.RUNNING;
            case UNRESOLVED -> next.state() == Scheduler.State.SUCCEEDED || next.state() == Scheduler.State.FAILED || next.state() == Scheduler.State.CANCELLED;
            default -> false;
        };
        if (!legal) throw new IOException("Invalid lifecycle history");
    }
    @Override public void close() {
        if (closed) return;
        closed = true;
        try { lock.release(); } catch (IOException failure) { throw new UncheckedIOException(failure); }
        finally {
            try { channel.close(); } catch (IOException failure) { throw new UncheckedIOException(failure); }
        }
    }
}
