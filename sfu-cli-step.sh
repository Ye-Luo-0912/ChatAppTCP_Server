cd ~/sfu-validation && curl -sL https://api.github.com/repos/livekit/livekit-cli/releases/latest -o cli.json && python3 -c "
import json
d = json.load(open('cli.json'))
print(d.get('tag_name'))
for a in d.get('assets', []):
    n = a['name']
    if 'linux_amd64' in n and n.endswith('.tar.gz'):
        print(a['browser_download_url'])
"
