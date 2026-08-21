# Pinned per Hyperlift's own recommendation, and matching the version the
# asset server migration targets.
FROM python:3.12-slim

WORKDIR /app
COPY probe.py probe-data.txt ./

# NOTE: no EXPOSE. Hyperlift's docs say to use the port environment variable
# instead of exposing in the Dockerfile.
#
# NOTE: locale is deliberately NOT forced here. probe.py reports the encoding
# actually in effect, so /locale tells us whether the base image default is
# already UTF-8. Set LC_ALL=C in Hyperlift's env vars to prove the failure
# can still be induced — which doubles as a test that env vars reach the
# process.

CMD ["python3", "-u", "probe.py"]
