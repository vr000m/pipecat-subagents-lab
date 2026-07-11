# Pipecat Subagents Lab

Experimental Pipecat-native voice assistants with persistent specialist subagents and a lightweight browser RTVI client.

The first experiment will route requests through a tool-free main model to persistent context-owning subagents, speak concise answers through a local TTS server, and render structured web-search results plus interruption state in a plain browser client. Electron packaging is deferred until the browser protocol and interaction model are proven.

## Planned layout

```text
server/          Python and Pipecat runtime
web/             Bun-managed plain HTML, JavaScript, and CSS RTVI client
shared/          Shared message schemas and protocol documentation
docs/dev_plans/  Reviewed implementation plans
```
