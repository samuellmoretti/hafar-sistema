@echo off
python -c "import urllib.request; print(urllib.request.urlopen('http://www.duckdns.org/update?domains=hafar&token=SEUTOKEN&ip=').read().decode())"
pause