import React, { useState, useEffect, useCallback } from "react";
import "./App.css";

const BlitzQueueVisualizer = () => {
  // Estado principal
  const [queues, setQueues] = useState(new Map());
  const [selectedQueue, setSelectedQueue] = useState(null);
  const [consumedMessages, setConsumedMessages] = useState([]);
  const [pendingConfirmations, setPendingConfirmations] = useState([]);
  const [confirmedMessages, setConfirmedMessages] = useState(new Set());
  const [systemStatus, setSystemStatus] = useState(
    "🎯 BlitzQueue Visualizer initialized! Loading existing queues..."
  );

  // Estados dos modais
  const [showCreateQueueModal, setShowCreateQueueModal] = useState(false);
  const [showSendMessageModal, setShowSendMessageModal] = useState(false);

  // Estados dos formulários
  const [queueForm, setQueueForm] = useState({
    name: "",
    type: 0,
    useUniqueMessage: false,
  });

  const [messageForm, setMessageForm] = useState({
    targetQueue: "",
    data: "",
    priority: 0,
    subQueue: "",
    deduplicationKey: "",
  });

  const baseUrl = "http://localhost:52525";

  // Funções utilitárias
  const getQueueTypeName = (type) => {
    const types = {
      0: "Standard",
      1: "FIFO",
      2: "Priority",
      3: "Scheduled",
    };
    return types[type] || "Unknown";
  };

  const updateSystemStatus = (message) => {
    setSystemStatus(message);
  };

  // Carregar filas existentes
  const loadExistingQueues = useCallback(async () => {
    try {
      const response = await fetch(`${baseUrl}/queues`);

      if (!response.ok) {
        console.log("No existing queues found or server not available");
        updateSystemStatus(
          "🎯 BlitzQueue Visualizer ready! Create your first queue to get started."
        );
        return;
      }

      const queueList = await response.json();

      if (queueList && queueList.length > 0) {
        console.log(`Found ${queueList.length} existing queues:`, queueList);

        const newQueues = new Map();
        queueList.forEach((queue) => {
          newQueues.set(queue.Name, queue);
          console.log(
            `Loaded queue: ${queue.Name} (${getQueueTypeName(queue.Type)})`
          );
        });

        setQueues(newQueues);
        updateSystemStatus(
          `✅ Loaded ${queueList.length} existing queue${
            queueList.length > 1 ? "s" : ""
          }! Ready to process messages.`
        );
      } else {
        updateSystemStatus(
          "🎯 BlitzQueue Visualizer ready! Create your first queue to get started."
        );
      }
    } catch (error) {
      console.error("Error loading existing queues:", error);
      updateSystemStatus(
        "⚠️ Could not load existing queues. Server may not be running."
      );
    }
  }, [baseUrl]);

  // Atualizar dados das filas
  const updateQueues = useCallback(async () => {
    const updatedQueues = new Map();

    for (const [queueName, queue] of queues) {
      try {
        const response = await fetch(`${baseUrl}/queue/${queueName}/peek`);
        if (response.ok) {
          const messages = await response.json();
          updatedQueues.set(queueName, {
            ...queue,
            messages: messages || [],
          });
        } else {
          updatedQueues.set(queueName, queue);
        }
      } catch (error) {
        console.error(`Error updating queue ${queueName}:`, error);
        updatedQueues.set(queueName, queue);
      }
    }

    setQueues(updatedQueues);
  }, [queues, baseUrl]);

  // Criar fila
  const createQueue = async () => {
    if (!queueForm.name.trim()) {
      alert("Please enter a queue name");
      return;
    }

    try {
      const response = await fetch(`${baseUrl}/queue`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          Name: queueForm.name.toLowerCase(),
          Type: parseInt(queueForm.type),
          UseUniqueMessage: queueForm.useUniqueMessage,
        }),
      });

      if (response.ok) {
        const newQueue = await response.json();
        setQueues((prev) => new Map(prev.set(newQueue.Name, newQueue)));
        updateSystemStatus(
          `✅ Created queue "${newQueue.Name}" (${getQueueTypeName(
            newQueue.Type
          )})`
        );
        setShowCreateQueueModal(false);
        setQueueForm({ name: "", type: 0, useUniqueMessage: false });
      } else {
        const error = await response.text();
        alert(`Failed to create queue: ${error}`);
      }
    } catch (error) {
      console.error("Error creating queue:", error);
      alert("Failed to create queue");
    }
  };

  // Enviar mensagem
  const sendMessageToQueue = async () => {
    if (!messageForm.targetQueue) {
      alert("Please select a target queue");
      return;
    }

    if (!messageForm.data.trim()) {
      alert("Please enter message data");
      return;
    }

    const targetQueue = queues.get(messageForm.targetQueue);
    if (!targetQueue) {
      alert("Selected queue not found");
      return;
    }

    try {
      const messageRequest = {
        Data: messageForm.data,
        DeduplicationKey: messageForm.deduplicationKey || messageForm.data,
      };

      if (targetQueue.Type === 2 && messageForm.priority) {
        messageRequest.Priority = parseInt(messageForm.priority);
      }

      if (targetQueue.Type === 1 && messageForm.subQueue) {
        messageRequest.SubQueue = messageForm.subQueue;
      }

      const response = await fetch(
        `${baseUrl}/queue/${messageForm.targetQueue}/push`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify([messageRequest]),
        }
      );

      if (response.ok) {
        updateSystemStatus(
          `📤 Message sent to queue "${messageForm.targetQueue}"`
        );
        setShowSendMessageModal(false);
        setMessageForm({
          targetQueue: "",
          data: "",
          priority: 0,
          subQueue: "",
          deduplicationKey: "",
        });
        await updateQueues();
      } else {
        alert("Failed to send message");
      }
    } catch (error) {
      console.error("Error sending message:", error);
      alert("Failed to send message");
    }
  };

  // Consumir mensagens
  const consumeMessages = async () => {
    if (!selectedQueue) {
      alert("Please select a queue first");
      return;
    }

    try {
      const response = await fetch(
        `${baseUrl}/queue/${selectedQueue.Name}/consume`
      );

      if (response.ok) {
        const messages = await response.json();
        if (messages && messages.length > 0) {
          setConsumedMessages((prev) => [...prev, ...messages]);
          setPendingConfirmations((prev) => [
            ...prev,
            ...messages.map((msg) => msg.Id),
          ]);
          updateSystemStatus(
            `🍽️ Consumed ${messages.length} message(s) from queue "${selectedQueue.Name}". Don't forget to confirm!`
          );
          await updateQueues();
        } else {
          updateSystemStatus(
            `📭 No messages to consume from queue "${selectedQueue.Name}"`
          );
        }
      }
    } catch (error) {
      console.error("Error consuming messages:", error);
      alert("Failed to consume messages");
    }
  };

  // Confirmar mensagens
  const confirmMessages = async () => {
    if (pendingConfirmations.length === 0) {
      alert("No messages to confirm!");
      return;
    }

    try {
      const response = await fetch(
        `${baseUrl}/queue/${selectedQueue.Name}/confirm`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            MessageIds: pendingConfirmations,
          }),
        }
      );

      if (response.ok) {
        // Adicionar mensagens confirmadas ao set
        setConfirmedMessages((prev) => {
          const newSet = new Set(prev);
          pendingConfirmations.forEach((id) => newSet.add(id));
          return newSet;
        });

        updateSystemStatus(
          `✅ Confirmed ${pendingConfirmations.length} message(s) in queue "${selectedQueue.Name}". Messages now appear in blue!`
        );
        setPendingConfirmations([]);
        await updateQueues();
      }
    } catch (error) {
      console.error("Error confirming messages:", error);
      alert("Failed to confirm messages");
    }
  };

  // Peek mensagens
  const peekMessages = async () => {
    if (!selectedQueue) {
      alert("Please select a queue first");
      return;
    }

    await updateQueues();
    updateSystemStatus(`👀 Peeked at queue "${selectedQueue.Name}"`);
  };

  // Renderizar mensagem
  const renderMessage = (msg, queueType) => {
    const isPriority = queueType === 2 && msg.Priority && msg.Priority > 0;
    const isProcessing = msg.Status === 1;
    const isConfirmed = confirmedMessages.has(msg.Id);

    let cssClass = "message";
    if (isPriority) cssClass += " priority";
    if (isProcessing) cssClass += " processing";
    if (isConfirmed) cssClass += " confirmed";

    const displayData =
      msg.Data && msg.Data.length > 30
        ? msg.Data.substring(0, 30) + "..."
        : msg.Data || `Msg-${msg.Id}`;

    return (
      <div
        key={msg.Id}
        className={cssClass}
        title={`${msg.Data || msg.Id}${isConfirmed ? " (Confirmed)" : ""}`}
      >
        {isPriority && <div className="message-priority">{msg.Priority}</div>}
        {isConfirmed && <div className="message-confirmed-badge">✓</div>}
        {displayData}
      </div>
    );
  };

  // Selecionar fila
  const selectQueue = (queueName) => {
    const queue = queues.get(queueName);
    setSelectedQueue(queue);
    updateSystemStatus(
      `📋 Selected queue: "${queueName}". You can now send messages or perform operations.`
    );
  };

  // Effects
  useEffect(() => {
    loadExistingQueues();
  }, [loadExistingQueues]);

  useEffect(() => {
    const interval = setInterval(updateQueues, 3000);
    return () => clearInterval(interval);
  }, [updateQueues]);

  // Atualizar dropdown de filas no modal de envio
  useEffect(() => {
    if (showSendMessageModal && selectedQueue) {
      setMessageForm((prev) => ({ ...prev, targetQueue: selectedQueue.Name }));
    }
  }, [showSendMessageModal, selectedQueue]);

  const hasPendingConfirmations = pendingConfirmations.length > 0;

  return (
    <div className="container">
      <h1>🚀 BlitzQueue Visualizer</h1>

      {/* Controls */}
      <div className="controls">
        <button onClick={() => setShowCreateQueueModal(true)}>
          ➕ Create Queue
        </button>
        <button
          onClick={() => setShowSendMessageModal(true)}
          disabled={!selectedQueue}
        >
          📤 Send Message
        </button>
        <button onClick={peekMessages} disabled={!selectedQueue}>
          👀 Peek Messages
        </button>
        <button onClick={consumeMessages} disabled={!selectedQueue}>
          🍽️ Consume Messages
        </button>
        <button onClick={confirmMessages} disabled={!hasPendingConfirmations}>
          ✅ Confirm Messages
        </button>
      </div>

      {/* Create Queue Modal */}
      {showCreateQueueModal && (
        <div
          className="modal"
          onClick={(e) =>
            e.target.className === "modal" && setShowCreateQueueModal(false)
          }
        >
          <div className="modal-content">
            <h3>Create New Queue</h3>
            <div className="form-group">
              <label>Queue Name:</label>
              <input
                type="text"
                value={queueForm.name}
                onChange={(e) =>
                  setQueueForm((prev) => ({ ...prev, name: e.target.value }))
                }
                placeholder="Enter queue name"
              />
            </div>
            <div className="form-group">
              <label>Queue Type:</label>
              <select
                value={queueForm.type}
                onChange={(e) =>
                  setQueueForm((prev) => ({
                    ...prev,
                    type: parseInt(e.target.value),
                  }))
                }
              >
                <option value="0">Standard (FIFO)</option>
                <option value="1">FIFO (Ordered)</option>
                <option value="2">Priority</option>
                <option value="3">Scheduled</option>
              </select>
            </div>
            <div className="form-group">
              <label>
                <input
                  type="checkbox"
                  checked={queueForm.useUniqueMessage}
                  onChange={(e) =>
                    setQueueForm((prev) => ({
                      ...prev,
                      useUniqueMessage: e.target.checked,
                    }))
                  }
                />
                Use Unique Messages (Deduplication)
              </label>
            </div>
            <div className="modal-actions">
              <button onClick={createQueue}>Create</button>
              <button onClick={() => setShowCreateQueueModal(false)}>
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Send Message Modal */}
      {showSendMessageModal && (
        <div
          className="modal"
          onClick={(e) =>
            e.target.className === "modal" && setShowSendMessageModal(false)
          }
        >
          <div className="modal-content">
            <h3>Send Message</h3>
            <div className="form-group">
              <label>Select Queue:</label>
              <select
                value={messageForm.targetQueue}
                onChange={(e) =>
                  setMessageForm((prev) => ({
                    ...prev,
                    targetQueue: e.target.value,
                  }))
                }
              >
                <option value="">Choose a queue...</option>
                {Array.from(queues.entries()).map(([name, queue]) => (
                  <option key={name} value={name}>
                    {name} ({getQueueTypeName(queue.Type)})
                  </option>
                ))}
              </select>
            </div>
            <div className="form-group">
              <label>Message Data:</label>
              <textarea
                value={messageForm.data}
                onChange={(e) =>
                  setMessageForm((prev) => ({ ...prev, data: e.target.value }))
                }
                placeholder="Enter message content"
              />
            </div>
            {messageForm.targetQueue &&
              queues.get(messageForm.targetQueue)?.Type === 2 && (
                <div className="form-group">
                  <label>Priority (higher = more priority):</label>
                  <input
                    type="number"
                    value={messageForm.priority}
                    onChange={(e) =>
                      setMessageForm((prev) => ({
                        ...prev,
                        priority: e.target.value,
                      }))
                    }
                    min="0"
                    max="10"
                  />
                </div>
              )}
            {messageForm.targetQueue &&
              queues.get(messageForm.targetQueue)?.Type === 1 && (
                <div className="form-group">
                  <label>Sub-Queue (for FIFO):</label>
                  <input
                    type="text"
                    value={messageForm.subQueue}
                    onChange={(e) =>
                      setMessageForm((prev) => ({
                        ...prev,
                        subQueue: e.target.value,
                      }))
                    }
                    placeholder="Sub-queue identifier"
                  />
                </div>
              )}
            <div className="form-group">
              <label>Deduplication Key (optional):</label>
              <input
                type="text"
                value={messageForm.deduplicationKey}
                onChange={(e) =>
                  setMessageForm((prev) => ({
                    ...prev,
                    deduplicationKey: e.target.value,
                  }))
                }
                placeholder="Leave empty to use message data"
              />
            </div>
            <div className="modal-actions">
              <button onClick={sendMessageToQueue}>Send</button>
              <button onClick={() => setShowSendMessageModal(false)}>
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Main Visualization */}
      <div className="blitzqueue-system">
        {/* Producer */}
        <div className="producer-section">
          <h3>📡 Producer</h3>
          <div className="producer-area">
            <div className="producer-status">Ready to send messages</div>
          </div>
        </div>

        {/* Queues */}
        <div className="queue-section">
          <h3>🗃️ Queues</h3>
          <div className="queue-container">
            {queues.size === 0 ? (
              <div className="no-queues">
                No queues created yet. Click "Create Queue" to start!
              </div>
            ) : (
              Array.from(queues.entries()).map(([name, queue]) => (
                <div
                  key={name}
                  className={`queue ${
                    selectedQueue?.Name === name ? "selected" : ""
                  }`}
                  onClick={() => selectQueue(name)}
                >
                  <div className="queue-header">
                    <div className="queue-title">{name}</div>
                    <div className="queue-type">
                      {getQueueTypeName(queue.Type)}
                    </div>
                  </div>
                  <div className="messages">
                    {!queue.messages || queue.messages.length === 0 ? (
                      <div
                        style={{
                          color: "#6c757d",
                          fontStyle: "italic",
                          textAlign: "center",
                          width: "100%",
                        }}
                      >
                        No messages
                      </div>
                    ) : (
                      queue.messages.map((msg) =>
                        renderMessage(msg, queue.Type)
                      )
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Consumer */}
        <div className="consumer-section">
          <h3>🍽️ Consumer</h3>
          <div className="consumer-area">
            <div className="consumer-status">
              Waiting for messages to consume
            </div>
            <div className="consumed-messages">
              {consumedMessages.map((msg) =>
                renderMessage(msg, selectedQueue?.Type || 0)
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Status */}
      <div className="explanation">
        <h3>📊 System Status</h3>
        <div id="systemStatus">{systemStatus}</div>
      </div>

      {/* Selected Queue Info */}
      {selectedQueue && (
        <div className="selected-queue-info">
          <h4>
            Selected Queue: <span>{selectedQueue.Name}</span>
          </h4>
          <div className="queue-stats">
            <div>
              Type: <span>{getQueueTypeName(selectedQueue.Type)}</span>
            </div>
            <div>
              Messages: <span>{selectedQueue.messages?.length || 0}</span>
            </div>
            <div>
              Unique Messages:{" "}
              <span>{selectedQueue.UseUniqueMessage ? "Yes" : "No"}</span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default BlitzQueueVisualizer;
