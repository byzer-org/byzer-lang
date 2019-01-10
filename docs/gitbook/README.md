
# Prerequisites

our project documents generate by `gitbook` tool.

1. install from `npm`

```bash
npm install -g gitbook-cli
```

2. run `gitbook` command with docker.

first, build gitbook docker image. 

```bash
docker build -t gitbook:v1 docs/gitbook/dev
```

# Generating the Documents HTML

- Generate Chinese documents html.
```bash
docker run --rm -v /Users/fchen/Project/streamingpro/docs/gitbook/:/tmp gitbook:v1 gitbook build /tmp/zh
```

- Generate English documents html.
```bash
docker run --rm -v /Users/fchen/Project/streamingpro/docs/gitbook/:/tmp gitbook:v1 gitbook build /tmp/en
```
