#!/bin/bash

SESSION_NAME="parallel-ai-dev"

# Check if session already exists
tmux has-session -t $SESSION_NAME 2>/dev/null

if [ $? != 0 ]; then
    # Create new session with first window
    tmux new-session -d -s $SESSION_NAME -n "parallel-ai"

    # Split window horizontally - top for Claude, bottom for server/shell
    tmux split-window -v -t $SESSION_NAME:0

    # Split bottom pane vertically - left for server logs, right for manual commands
    tmux split-window -h -t $SESSION_NAME:0.1

    # Adjust pane sizes: 60% top (Claude), 40% bottom split between server and shell
    tmux resize-pane -t $SESSION_NAME:0.0 -y 60%

    # Top pane (0.0): Claude CLI - your main workspace
    tmux send-keys -t $SESSION_NAME:0.0 "cd $(pwd) && claude" C-m

    # Bottom left pane (0.1): Server logs
    tmux send-keys -t $SESSION_NAME:0.1 "cd $(pwd) && echo 'Run: ./start_server.sh' && echo 'Server will display here'" C-m

    # Bottom right pane (0.2): Shell for manual commands
    tmux send-keys -t $SESSION_NAME:0.2 "cd $(pwd)" C-m

    # Select the Claude pane to start
    tmux select-pane -t $SESSION_NAME:0.0

    echo "✅ Created tmux session '$SESSION_NAME'"
    echo "   Layout:"
    echo "   ┌─────────────────────────────────┐"
    echo "   │   Claude CLI (main workspace)   │ 60%"
    echo "   ├──────────────┬──────────────────┤"
    echo "   │ Server Logs  │  Shell/Commands  │ 40%"
    echo "   └──────────────┴──────────────────┘"
    echo ""
    echo "   Start server in bottom-left pane: ./start_server.sh"
else
    echo "⚠️  Session '$SESSION_NAME' already exists"
fi

# Attach to the session
tmux attach-session -t $SESSION_NAME
