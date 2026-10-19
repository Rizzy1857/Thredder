import os, subprocess, json, urllib.request, sys
result=subprocess.run(['git','credential','fill'],input='protocol=https\nhost=github.com\n\n',text=True,capture_output=True,env={**os.environ,'GIT_TERMINAL_PROMPT':'0'},timeout=30)
if result.returncode:
    print('GitHub credentials are not available through the configured Git credential helper.')
    sys.exit(2)
credentials=dict(line.split('=',1) for line in result.stdout.splitlines() if '=' in line)
token=credentials.get('password')
if not token:
    print('No GitHub access token returned by credential helper.');sys.exit(2)
request=urllib.request.Request('https://api.github.com/user',headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json','User-Agent':'Thredder-release-publisher'})
try:
    with urllib.request.urlopen(request,timeout=20) as response:
        print(json.dumps({'authenticated_login':json.load(response)['login']}))
except Exception as error:
    print(type(error).__name__+': GitHub authentication check failed');sys.exit(3)
