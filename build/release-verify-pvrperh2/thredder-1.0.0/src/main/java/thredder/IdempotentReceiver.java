package thredder;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32;
import static java.nio.file.StandardOpenOption.*;

/** Local transactional receiver demo. Owns both balances and deduplication in one atomic snapshot. */
public final class IdempotentReceiver implements AutoCloseable {
    public record Request(String key, String from, String to, long amount) {
        public Request {
            if (key == null || key.isBlank() || key.length() > 200) throw new IllegalArgumentException("Key must contain 1..200 characters");
            if (!("A".equals(from) || "B".equals(from)) || !("A".equals(to) || "B".equals(to)) || from.equals(to))
                throw new IllegalArgumentException("Transfer must be between distinct accounts A and B");
            if (amount <= 0) throw new IllegalArgumentException("Amount must be positive");
        }
    }
    public record Result(String key, long balanceA, long balanceB) { }
    private record Completed(Request request, Result result) { }
    interface Probe { void at(String boundary); }
    private static final int MAGIC = 0x54494435;
    private final Path file;
    private final FileChannel owner;
    private final FileLock lock;
    private final Probe probe;
    private Map<String, Completed> completed = new LinkedHashMap<>();
    private long balanceA = 10_000, balanceB = 10_000;
    private boolean unavailable;

    public IdempotentReceiver(Path file) { this(file, boundary -> {}); }
    IdempotentReceiver(Path file, Probe probe) {
        this.file = file.toAbsolutePath(); this.probe = probe;
        FileChannel opened = null; FileLock held = null;
        try {
            if (!Files.isDirectory(this.file.getParent())) throw new IOException("Receiver parent directory must exist");
            opened = FileChannel.open(Path.of(this.file + ".lock"), CREATE, WRITE);
            held = opened.tryLock();
            if (held == null) throw new IOException("Receiver is already owned by another process");
            owner = opened; lock = held;
            if (Files.exists(this.file)) load();
            else commit(new LinkedHashMap<>(), balanceA, balanceB);
        } catch (IOException | RuntimeException failure) {
            if (held != null) try { held.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            if (opened != null) try { opened.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            if (failure instanceof IOException io) throw new UncheckedIOException(io);
            throw (RuntimeException) failure;
        }
    }
    /** Concurrent duplicate requests serialize at the same boundary as the effect. */
    public synchronized Result transfer(Request request) {
        ensureAvailable(); Objects.requireNonNull(request);
        Completed prior = completed.get(request.key());
        if (prior != null) {
            if (!prior.request().equals(request)) throw new IllegalArgumentException("Idempotency key already belongs to a different payload");
            return prior.result();
        }
        long source = request.from().equals("A") ? balanceA : balanceB;
        if (request.amount() > source) throw new IllegalArgumentException("Insufficient balance");
        long nextA = request.from().equals("A") ? balanceA - request.amount() : balanceA + request.amount();
        long nextB = 20_000 - nextA;
        Result result = new Result(request.key(), nextA, nextB);
        Map<String, Completed> next = new LinkedHashMap<>(completed);
        next.put(request.key(), new Completed(request, result));
        try {
            commit(next, nextA, nextB);
            completed = next; balanceA = nextA; balanceB = nextB;
            probe.at("after-commit-before-response");
            return result;
        } catch (IOException failure) {
            unavailable = true;
            throw new UncheckedIOException("Commit outcome may be uncertain; reopen receiver and retry the SAME key/payload", failure);
        } catch (RuntimeException failure) {
            unavailable = true; throw failure;
        }
    }
    /** Absence is only a local observation; it is not permission to retry using a new key. */
    public synchronized Optional<Result> lookup(String key) {
        ensureAvailable(); Completed entry = completed.get(key);
        return entry == null ? Optional.empty() : Optional.of(entry.result());
    }
    public synchronized long balance(String account) {
        ensureAvailable();
        return switch (account) { case "A" -> balanceA; case "B" -> balanceB;
            default -> throw new IllegalArgumentException("Unknown account"); };
    }
    private void ensureAvailable() {
        if (unavailable || !owner.isOpen()) throw new IllegalStateException("Receiver unavailable; reopen and reconcile using the same key");
    }
    private void commit(Map<String, Completed> records, long a, long b) throws IOException {
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(payload)) {
            out.writeInt(MAGIC); out.writeLong(a); out.writeLong(b); out.writeInt(records.size());
            for (Completed c : records.values()) {
                out.writeUTF(c.request().key()); out.writeUTF(c.request().from()); out.writeUTF(c.request().to());
                out.writeLong(c.request().amount()); out.writeLong(c.result().balanceA()); out.writeLong(c.result().balanceB());
            }
        }
        byte[] body = payload.toByteArray(); CRC32 crc = new CRC32(); crc.update(body);
        Path temporary = Files.createTempFile(file.getParent(), file.getFileName().toString() + ".", ".pending");
        try {
            try (FileChannel channel = FileChannel.open(temporary, WRITE)) {
                ByteBuffer bytes = ByteBuffer.allocate(body.length + 8).put(body).putLong(crc.getValue()); bytes.flip();
                while (bytes.hasRemaining()) channel.write(bytes);
                channel.force(true);
            }
            probe.at("before-replace");
            Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            probe.at("after-replace");
            try (FileChannel directory = FileChannel.open(file.getParent(), READ)) { directory.force(true); }
        } finally { Files.deleteIfExists(temporary); }
    }
    private void load() throws IOException {
        byte[] bytes = Files.readAllBytes(file);
        if (bytes.length < 32) throw new IOException("Truncated receiver snapshot");
        CRC32 crc = new CRC32(); crc.update(bytes, 0, bytes.length - 8);
        if (ByteBuffer.wrap(bytes, bytes.length - 8, 8).getLong() != crc.getValue()) throw new IOException("Receiver checksum mismatch");
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, 0, bytes.length - 8))) {
            if (in.readInt() != MAGIC) throw new IOException("Unknown receiver format");
            long savedA = in.readLong(), savedB = in.readLong(); int count = in.readInt();
            if (count < 0 || count > bytes.length / 30) throw new IOException("Invalid receiver entry count");
            long a = 10_000, b = 10_000;
            Map<String, Completed> records = new LinkedHashMap<>();
            for (int i = 0; i < count; i++) {
                Request r = new Request(in.readUTF(), in.readUTF(), in.readUTF(), in.readLong());
                if (r.amount() > (r.from().equals("A") ? a : b)) throw new IOException("Invalid transfer history");
                a += r.from().equals("A") ? -r.amount() : r.amount(); b = 20_000 - a;
                Result result = new Result(r.key(), in.readLong(), in.readLong());
                if (result.balanceA() != a || result.balanceB() != b || records.put(r.key(), new Completed(r, result)) != null)
                    throw new IOException("Conflicting completion history");
            }
            if (a != savedA || b != savedB || in.available() != 0) throw new IOException("Invalid receiver balances/trailing data");
            completed = records; balanceA = a; balanceB = b;
        } catch (IllegalArgumentException failure) { throw new IOException("Invalid receiver request", failure); }
    }
    @Override public synchronized void close() {
        if (!owner.isOpen()) return;
        unavailable = true;
        try { lock.release(); }
        catch (IOException failure) { throw new UncheckedIOException(failure); }
        finally { try { owner.close(); } catch (IOException failure) { throw new UncheckedIOException(failure); } }
    }
}
