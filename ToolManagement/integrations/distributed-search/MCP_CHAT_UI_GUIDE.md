# MCP Chat UI - Complete Guide

## Overview

A beautiful, modern web UI for the MCP Chat FastAPI server, built with **Datastar** for reactive, hypermedia-driven interactions. Features real-time streaming chat, file uploads, MCP tool integration, and a gorgeous gradient design.

## Features

### ✨ Core Features
- 💬 **Real-time Streaming Chat** - SSE-powered streaming responses
- 🔧 **MCP Tool Integration** - Seamlessly call tools from connected MCP servers
- 📎 **File Uploads** - Upload and attach files to your messages
- 🎯 **Multi-turn Conversations** - Full conversation history management
- 🎨 **Beautiful UI** - Modern gradient design with Tailwind CSS
- ⚡ **Reactive** - Datastar-powered reactivity without heavy frameworks
- 🌐 **No Build Step** - Pure HTML/JS, runs instantly

### 🎨 UI Components

1. **Header**
   - Gradient background (purple to violet)
   - New conversation button
   - Tools toggle switch
   - Branding and logo

2. **Chat Area**
   - Welcome screen with feature highlights
   - Message bubbles (purple for user, gray for assistant)
   - Tool call badges
   - Thinking indicators
   - Typing animation while loading
   - Auto-scroll to latest message

3. **File Upload**
   - Drag & drop area (via button)
   - Multiple file support
   - File preview badges
   - Remove file option
   - Base64 encoding for transmission

4. **Input Area**
   - File attachment button
   - Text input with focus states
   - Send button with icon
   - Disabled states while loading

## Architecture

```
┌─────────────────────────────────────────┐
│         Browser (Datastar UI)           │
│                                         │
│  ┌──────────────────────────────────┐  │
│  │  Signals (Reactive State)        │  │
│  │  - messages[]                    │  │
│  │  - currentMessage                │  │
│  │  - conversationId                │  │
│  │  - isLoading                     │  │
│  │  - toolsEnabled                  │  │
│  │  - uploadedFiles[]               │  │
│  └──────────────────────────────────┘  │
│                                         │
│  ┌──────────────────────────────────┐  │
│  │  Event Handlers                  │  │
│  │  - File upload                   │  │
│  │  - Form submit                   │  │
│  │  - SSE stream processing         │  │
│  └──────────────────────────────────┘  │
└─────────────────────────────────────────┘
              │
              │ POST /chat (SSE)
              ▼
┌─────────────────────────────────────────┐
│      FastAPI Backend                    │
│  - MCP client integration               │
│  - OpenRouter LLM streaming             │
│  - Tool execution                       │
│  - File processing                      │
└─────────────────────────────────────────┘
```

## Quick Start

### 1. Start the Server

```bash
# Set environment variables
export OPENROUTER_API_KEY="your-key"
export JINA_API_KEY="your-jina-key"

# Start server
./start_server.sh

# Or manually:
python3 fastapi_mcp_chat.py
```

Server starts on: **http://localhost:2046**

### 2. Open Browser

Navigate to: **http://localhost:2046**

The UI loads instantly - no build step required!

### 3. Start Chatting

1. Type a message in the input box
2. (Optional) Click the file button to attach files
3. (Optional) Toggle "Tools" to enable/disable MCP tools
4. Click "Send" or press Enter
5. Watch the streaming response appear in real-time!

## Datastar Integration

### What is Datastar?

Datastar is a hypermedia-driven framework that brings reactivity to HTML using attributes. It's like Alpine.js but with first-class support for server-sent events and backend integration.

### Key Datastar Features Used

1. **Signals** (Reactive State)
   ```html
   <body data-signals='{"messages":[], "currentMessage":"", ...}'>
   ```

2. **Data Binding**
   ```html
   <input data-model="currentMessage" />
   <div data-text="msg.content"></div>
   ```

3. **Event Handlers**
   ```html
   <button data-on-click="$$messages=[];$$conversationId=null">
     New Chat
   </button>
   ```

4. **Conditional Rendering**
   ```html
   <div data-show="$messages.length === 0">
     Welcome screen
   </div>
   ```

5. **Loops**
   ```html
   <template data-for="msg in $messages">
     <div data-text="msg.content"></div>
   </template>
   ```

### Why Datastar?

- ✅ **No Build Step** - Works directly in the browser
- ✅ **SSE Native** - Perfect for streaming responses
- ✅ **Lightweight** - Minimal JavaScript overhead
- ✅ **Reactive** - Automatic UI updates on state changes
- ✅ **Hypermedia-driven** - Follows modern web standards

## File Upload Flow

```
1. User selects file
   ↓
2. FileReader converts to base64
   ↓
3. Added to uploadedFiles signal
   ↓
4. Displayed as badge in UI
   ↓
5. Included in chat request
   ↓
6. Server decodes and processes
   ↓
7. Content added to LLM context
   ↓
8. Files cleared after sending
```

### Supported File Types

- **Text Files**: `.txt`, `.md`, `.json`, `.csv`
  - Content included in prompt (up to 2000 chars)

- **Images**: `.jpg`, `.png`, `.gif`, `.webp`
  - Metadata included (filename, size)
  - Future: Vision API integration

- **Other Files**
  - Metadata only (filename, type, size)

## Event Types

The UI handles these SSE events:

### Session Events
- `session.start` - Captures conversation ID
- `session.update` - Updates session state
- `session.end` - Marks completion

### Message Events
- `message.start` - Prepares for new message
- `message.delta` - Appends streaming chunks
- `message.end` - Finalizes message

### Tool Events
- `tool.call.start` - Shows tool badge
- `tool.call.args` - Displays arguments
- `tool.call.execute` - Shows execution status
- `tool.result` - Displays result
- `tool.call.end` - Completes tool call

### Thinking Events
- `thinking.start` - Shows thinking indicator
- `thinking.delta` - Streams reasoning
- `thinking.end` - Completes thinking

## Customization

### Colors

Edit the CSS in the `<style>` section:

```css
.gradient-bg {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
}

/* User message color */
.bg-purple-600 { /* Change to your color */ }

/* Assistant message color */
.bg-gray-100 { /* Change to your color */ }
```

### Models

Change the default model in the signals:

```javascript
data-signals='{"...", "model":"anthropic/claude-3-opus", ...}'
```

Or add a model selector in the UI.

### Layout

Adjust chat container height:

```css
.chat-container {
    height: calc(100vh - 16rem); /* Adjust as needed */
}
```

### Features

Add new features by:
1. Adding signal fields
2. Creating UI controls
3. Updating the chat payload
4. Handling in backend

Example - Add "temperature" control:

```html
<!-- In header -->
<input type="range" data-model="temperature" min="0" max="2" step="0.1">

<!-- In signals -->
data-signals='{"...", "temperature": 0.7, ...}'

<!-- In chat handler -->
payload.temperature = signals.temperature;
```

## API Integration

### Request Format

```javascript
const payload = {
    message: "Your message",
    conversation_id: "conv_abc123",  // null for new
    model: "anthropic/claude-3.5-sonnet",
    stream: true,
    enable_tools: true,
    temperature: 0.7,
    max_tokens: 4000,
    files: [
        {
            name: "doc.txt",
            type: "text/plain",
            size: 1234,
            data: "base64encodedcontent"
        }
    ]
};

const response = await fetch('/chat', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
});
```

### Response Handling

```javascript
const reader = response.body.getReader();
const decoder = new TextDecoder();

while (true) {
    const {done, value} = await reader.read();
    if (done) break;

    const chunk = decoder.decode(value);
    const lines = chunk.split('\n');

    for (const line of lines) {
        if (line.startsWith('event:')) {
            // Event type
        }
        if (line.startsWith('data:')) {
            const data = JSON.parse(line.substring(5));
            // Handle event
        }
    }
}
```

## Troubleshooting

### UI doesn't load
- Check browser console for errors
- Verify Datastar CDN is accessible
- Check if server is running on port 2046

### Files not uploading
- Check file size (browsers limit FileReader)
- Verify base64 encoding in dev tools
- Check server logs for decode errors

### Messages not streaming
- Check SSE connection in Network tab
- Verify OPENROUTER_API_KEY is set
- Check for CORS issues

### Tools not working
- Verify MCP server is connected: `/api/status`
- Check tools are loaded: `/tools`
- Enable tools toggle in UI

### Styling issues
- Verify Tailwind CSS CDN is loaded
- Check for CSS conflicts
- Inspect elements in dev tools

## Performance

### Optimizations

1. **Message Limits**
   - Limit conversation history in backend
   - Truncate long messages in UI

2. **File Size**
   - Limit uploaded file size
   - Compress images before upload

3. **Debouncing**
   - Add debounce to rapid typing
   - Throttle UI updates

4. **Lazy Loading**
   - Load old messages on scroll
   - Paginate conversation list

### Metrics

- **Initial Load**: ~50KB (HTML + CDN)
- **Memory**: ~10-20MB for 100 messages
- **Network**: SSE stream ~1-5KB/s during generation

## Browser Support

- ✅ Chrome/Edge 90+
- ✅ Firefox 88+
- ✅ Safari 14+
- ✅ Opera 76+
- ❌ IE (not supported)

Requires:
- ES6+ JavaScript
- Fetch API
- SSE (EventSource)
- FileReader API

## Security

### Considerations

1. **File Uploads**
   - Validate file types
   - Limit file sizes
   - Scan for malware (production)

2. **XSS Prevention**
   - Sanitize user input
   - Use text binding (not innerHTML)
   - CSP headers

3. **API Keys**
   - Never expose in frontend
   - Use environment variables
   - Rotate regularly

4. **CORS**
   - Configure allowed origins
   - Use credentials carefully

## Next Steps

### Enhancements

- [ ] Message editing
- [ ] Message deletion
- [ ] Conversation search
- [ ] Export conversation
- [ ] Dark mode toggle
- [ ] Keyboard shortcuts
- [ ] Voice input
- [ ] Image preview
- [ ] Code syntax highlighting
- [ ] Markdown rendering
- [ ] User authentication
- [ ] Conversation sharing

## Resources

- [Datastar Documentation](https://data-star.dev)
- [FastAPI Docs](https://fastapi.tiangolo.com)
- [Tailwind CSS](https://tailwindcss.com)
- [OpenRouter API](https://openrouter.ai/docs)
- [MCP Specification](https://modelcontextprotocol.io)

## Support

For issues or questions:
1. Check server logs: `tail -f logs/*`
2. Check browser console
3. Review API responses in Network tab
4. Check environment variables

## License

Same as parent project
