class BlitzQueueVisualizer {
  constructor() {
    this.baseUrl = "http://localhost:52525";
    this.queues = new Map();
    this.selectedQueue = null;
    this.consumedMessages = [];
    this.pendingConfirmations = [];
    this.confirmedMessages = new Set(); // Track confirmed message IDs
    this.messageCounter = 0;
    this.init();
  }

  async init() {
    this.updateSystemStatus(
      "🎯 BlitzQueue Visualizer initialized! Loading existing queues..."
    );

    // Carregar filas existentes
    await this.loadExistingQueues();

    this.startPolling();
  }

  // Load existing queues from server
  async loadExistingQueues() {
    try {
      const response = await fetch(`${this.baseUrl}/queues`);

      if (!response.ok) {
        console.log("No existing queues found or server not available");
        this.updateSystemStatus(
          "🎯 BlitzQueue Visualizer ready! Create your first queue to get started."
        );
        return;
      }

      const queues = await response.json();

      if (queues && queues.length > 0) {
        console.log(`Found ${queues.length} existing queues:`, queues);

        // Add each queue to our map
        queues.forEach((queue) => {
          this.queues.set(queue.Name, queue);
          console.log(
            `Loaded queue: ${queue.Name} (${this.getQueueTypeName(queue.Type)})`
          );
        });

        // Update UI
        this.render();
        this.updateSystemStatus(
          `✅ Loaded ${queues.length} existing queue${
            queues.length > 1 ? "s" : ""
          }! Ready to process messages.`
        );
      } else {
        this.updateSystemStatus(
          "🎯 BlitzQueue Visualizer ready! Create your first queue to get started."
        );
      }
    } catch (error) {
      console.error("Error loading existing queues:", error);
      this.updateSystemStatus(
        "⚠️ Could not load existing queues. Server may not be running."
      );
    }
  }

  // Modal Management
  showCreateQueueModal() {
    document.getElementById("createQueueModal").style.display = "block";
    document.getElementById("queueName").focus();
  }

  hideCreateQueueModal() {
    document.getElementById("createQueueModal").style.display = "none";
    this.clearCreateQueueForm();
  }

  showSendMessageModal() {
    // Popular dropdown com filas disponíveis
    this.populateQueueDropdown();

    const modal = document.getElementById("sendMessageModal");
    const priorityGroup = document.getElementById("priorityGroup");
    const subQueueGroup = document.getElementById("subQueueGroup");

    // Se tiver fila selecionada, pré-selecionar no dropdown
    if (this.selectedQueue) {
      document.getElementById("targetQueue").value = this.selectedQueue.Name;
      this.updateModalFieldsBasedOnQueue(this.selectedQueue);
    } else {
      // Reset fields quando não há fila selecionada
      priorityGroup.style.display = "none";
      subQueueGroup.style.display = "none";
    }

    modal.style.display = "block";
    document.getElementById("messageData").focus();
  }

  populateQueueDropdown() {
    const dropdown = document.getElementById("targetQueue");
    dropdown.innerHTML = '<option value="">Choose a queue...</option>';

    this.queues.forEach((queue, name) => {
      const option = document.createElement("option");
      option.value = name;
      option.textContent = `${name} (${this.getQueueTypeName(queue.Type)})`;
      dropdown.appendChild(option);
    });

    // Adicionar event listener para mudança de fila
    dropdown.onchange = () => {
      const selectedQueueName = dropdown.value;
      if (selectedQueueName) {
        const selectedQueue = this.queues.get(selectedQueueName);
        this.updateModalFieldsBasedOnQueue(selectedQueue);
      }
    };
  }

  updateModalFieldsBasedOnQueue(queue) {
    const priorityGroup = document.getElementById("priorityGroup");
    const subQueueGroup = document.getElementById("subQueueGroup");

    // Show/hide fields based on queue type
    if (queue.Type === 2) {
      // Priority queue
      priorityGroup.style.display = "block";
    } else {
      priorityGroup.style.display = "none";
    }

    if (queue.Type === 1) {
      // FIFO queue
      subQueueGroup.style.display = "block";
    } else {
      subQueueGroup.style.display = "none";
    }
  }

  hideSendMessageModal() {
    document.getElementById("sendMessageModal").style.display = "none";
    this.clearSendMessageForm();
  }

  clearCreateQueueForm() {
    document.getElementById("queueName").value = "";
    document.getElementById("queueType").value = "0";
    document.getElementById("useUniqueMessage").checked = false;
  }

  clearSendMessageForm() {
    document.getElementById("targetQueue").value = "";
    document.getElementById("messageData").value = "";
    document.getElementById("messagePriority").value = "0";
    document.getElementById("messageSubQueue").value = "";
    document.getElementById("deduplicationKey").value = "";
  }

  // Queue Management
  async createQueue() {
    const name = document.getElementById("queueName").value.trim();
    const type = parseInt(document.getElementById("queueType").value);
    const useUniqueMessage =
      document.getElementById("useUniqueMessage").checked;

    if (!name) {
      alert("Please enter a queue name");
      return;
    }

    try {
      const response = await fetch(`${this.baseUrl}/queue`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          name: name,
          type: 0, // 0 = QueueTypeStandard (manter funcionamento original)
          useUniqueMessage: false,
        }),
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.message || "Failed to create queue");
      }

      const queue = await response.json();
      console.log("Queue created:", queue);

      // Adicionar a nova fila ao Map imediatamente
      this.queues.set(name, queue);
      console.log("Queues after adding:", this.queues);

      this.hideCreateQueueModal();

      // Chamar render diretamente primeiro
      this.render();

      await this.updateQueues();
      this.updateSystemStatus(
        `🎉 Queue "${name}" created successfully! Type: ${this.getQueueTypeName(
          0
        )}`
      );

      // Auto-select the newly created queue
      this.selectQueue(name);
    } catch (error) {
      console.error("Error creating queue:", error);
      alert(`Failed to create queue: ${error.message}`);
    }
  }

  selectQueue(queueName) {
    // Remove selection from all queues
    document
      .querySelectorAll(".queue")
      .forEach((q) => q.classList.remove("selected"));

    // Select the clicked queue
    const queueElement = document.querySelector(
      `[data-queue-name="${queueName}"]`
    );
    if (queueElement) {
      queueElement.classList.add("selected");
    }

    this.selectedQueue = this.queues.get(queueName);
    this.updateSelectedQueueInfo();
    this.updateButtons();
    this.updateSystemStatus(
      `📋 Selected queue: "${queueName}". You can now send messages or perform operations.`
    );
  }

  updateSelectedQueueInfo() {
    const info = document.getElementById("selectedQueueInfo");
    const nameSpan = document.getElementById("selectedQueueName");
    const typeSpan = document.getElementById("selectedQueueType");
    const countSpan = document.getElementById("selectedQueueMessageCount");
    const uniqueSpan = document.getElementById("selectedQueueUniqueMessages");

    if (this.selectedQueue) {
      info.style.display = "block";
      nameSpan.textContent = this.selectedQueue.Name;
      typeSpan.textContent = this.getQueueTypeName(this.selectedQueue.Type);
      countSpan.textContent = this.selectedQueue.messages
        ? this.selectedQueue.messages.length
        : 0;
      uniqueSpan.textContent = this.selectedQueue.UseUniqueMessage
        ? "Yes"
        : "No";
    } else {
      info.style.display = "none";
    }
  }

  updateButtons() {
    const hasSelectedQueue = !!this.selectedQueue;
    const hasQueues = this.queues.size > 0;
    const hasMessages =
      hasSelectedQueue &&
      this.selectedQueue.messages &&
      this.selectedQueue.messages.length > 0;
    const hasPendingConfirmations = this.pendingConfirmations.length > 0;

    // Permitir envio se tiver qualquer fila disponível
    document.getElementById("sendMessageBtn").disabled = !hasQueues;
    document.getElementById("peekBtn").disabled = !hasSelectedQueue;
    document.getElementById("consumeBtn").disabled = !hasMessages;
    document.getElementById("confirmBtn").disabled = !hasPendingConfirmations;
  }

  getQueueTypeName(type) {
    const types = ["Standard", "FIFO", "Priority", "Scheduled"];
    return types[type] || "Unknown";
  }

  // Message Management
  async sendMessage() {
    if (!this.selectedQueue) {
      this.showSendMessageModal();
      return;
    }
    this.showSendMessageModal();
  }

  async sendMessageToQueue() {
    const targetQueueName = document.getElementById("targetQueue").value;
    const messageData = document.getElementById("messageData").value.trim();
    const priority =
      parseInt(document.getElementById("messagePriority").value) || 0;
    const subQueue = document.getElementById("messageSubQueue").value.trim();
    const deduplicationKey = document
      .getElementById("deduplicationKey")
      .value.trim();

    if (!targetQueueName) {
      alert("Please select a queue");
      return;
    }

    if (!messageData) {
      alert("Please enter message content");
      return;
    }

    const targetQueue = this.queues.get(targetQueueName);
    if (!targetQueue) {
      alert("Selected queue not found");
      return;
    }

    try {
      // Animar da seção producer para a fila específica
      this.animateMessageFromProducerToQueue(messageData, targetQueueName);

      const message = {
        data: messageData,
      };

      if (targetQueue.Type === 2) {
        // Priority queue
        message.priority = priority;
      }

      if (targetQueue.Type === 1 && subQueue) {
        // FIFO queue
        message.subQueue = subQueue;
      }

      if (deduplicationKey) {
        message.deduplicationKey = deduplicationKey;
      }

      const response = await fetch(
        `${this.baseUrl}/queue/${targetQueueName}/push`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify([message]),
        }
      );

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.message || "Failed to send message");
      }

      this.hideSendMessageModal();
      await this.updateQueues();
      this.updateSystemStatus(
        `📤 Message sent successfully to queue "${targetQueueName}"!`
      );
    } catch (error) {
      console.error("Error sending message:", error);
      alert(`Failed to send message: ${error.message}`);
    }
  }

  async peekMessages() {
    if (!this.selectedQueue) {
      alert("Please select a queue first!");
      return;
    }

    try {
      const response = await fetch(
        `${this.baseUrl}/queue/${this.selectedQueue.Name}/peek`
      );
      if (response.ok) {
        const messages = await response.json();
        this.updateSystemStatus(
          `👀 Peeked at ${messages ? messages.length : 0} messages in queue "${
            this.selectedQueue.Name
          }". Messages remain in queue.`
        );
        await this.updateQueues();
      }
    } catch (error) {
      console.error("Error peeking messages:", error);
      alert("Failed to peek messages");
    }
  }

  async consumeMessages() {
    if (!this.selectedQueue) {
      alert("Please select a queue first!");
      return;
    }

    try {
      const response = await fetch(
        `${this.baseUrl}/queue/${this.selectedQueue.Name}/consume`
      );
      if (response.ok) {
        const messages = await response.json();
        if (messages && messages.length > 0) {
          // Animate messages moving to consumer
          messages.forEach((msg, index) => {
            setTimeout(() => {
              this.animateMessageToConsumer(msg);
            }, index * 200);
          });

          // Add to pending confirmations
          this.pendingConfirmations.push(...messages.map((msg) => msg.Id));
          this.updateButtons();
          this.updateSystemStatus(
            `🍽️ Consumed ${messages.length} message(s) from queue "${this.selectedQueue.Name}". Don't forget to confirm!`
          );
        } else {
          this.updateSystemStatus(
            `🍽️ No messages to consume from queue "${this.selectedQueue.Name}".`
          );
        }
        await this.updateQueues();
      }
    } catch (error) {
      console.error("Error consuming messages:", error);
      alert("Failed to consume messages");
    }
  }

  async confirmMessages() {
    if (this.pendingConfirmations.length === 0) {
      alert("No messages to confirm!");
      return;
    }

    try {
      const response = await fetch(
        `${this.baseUrl}/queue/${this.selectedQueue.Name}/confirm`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            MessageIds: this.pendingConfirmations,
          }),
        }
      );

      if (response.ok) {
        // Add confirmed messages to our set
        this.pendingConfirmations.forEach((messageId) => {
          this.confirmedMessages.add(messageId);
        });

        // Force immediate re-render to show visual changes
        this.render();

        this.updateSystemStatus(
          `✅ Confirmed ${this.pendingConfirmations.length} message(s) in queue "${this.selectedQueue.Name}". Messages now appear in blue!`
        );
        this.pendingConfirmations = [];
        this.updateButtons();
        await this.updateQueues();
      }
    } catch (error) {
      console.error("Error confirming messages:", error);
      alert("Failed to confirm messages");
    }
  }

  // Animation Methods
  animateMessageFromProducerToQueue(messageData, targetQueueName) {
    const producerArea = document.getElementById("producerArea");
    const queueElement = document.querySelector(
      `[data-queue-name="${targetQueueName}"]`
    );

    if (!queueElement) return;

    const message = document.createElement("div");
    message.className = "message moving";
    message.textContent =
      messageData.length > 20
        ? messageData.substring(0, 20) + "..."
        : messageData;

    document.body.appendChild(message);

    const producerRect = producerArea.getBoundingClientRect();
    const queueRect = queueElement.getBoundingClientRect();

    message.style.position = "fixed";
    message.style.left = `${producerRect.left + producerRect.width / 2}px`;
    message.style.top = `${producerRect.top + producerRect.height / 2}px`;
    message.style.transform = "translate(-50%, -50%)";

    setTimeout(() => {
      message.style.transition = "all 0.8s ease";
      message.style.left = `${queueRect.left + queueRect.width / 2}px`;
      message.style.top = `${queueRect.top + queueRect.height / 2}px`;

      setTimeout(() => {
        message.remove();
      }, 800);
    }, 50);
  }

  animateMessageFromProducer(messageData) {
    // Método legado - manter para compatibilidade
    if (this.selectedQueue) {
      this.animateMessageFromProducerToQueue(
        messageData,
        this.selectedQueue.Name
      );
    }
  }

  animateMessageToConsumer(messageData) {
    const queueElement = document.querySelector(
      `[data-queue-name="${this.selectedQueue.Name}"]`
    );
    const consumerArea = document.getElementById("consumerArea");

    if (!queueElement) return;

    const message = document.createElement("div");
    message.className = "message moving";
    message.textContent = messageData.Data
      ? messageData.Data.length > 20
        ? messageData.Data.substring(0, 20) + "..."
        : messageData.Data
      : `Msg-${messageData.Id}`;

    document.body.appendChild(message);

    const queueRect = queueElement.getBoundingClientRect();
    const consumerRect = consumerArea.getBoundingClientRect();

    message.style.position = "fixed";
    message.style.left = `${queueRect.left + queueRect.width / 2}px`;
    message.style.top = `${queueRect.top + queueRect.height / 2}px`;
    message.style.transform = "translate(-50%, -50%)";

    setTimeout(() => {
      message.style.transition = "all 0.8s ease";
      message.style.left = `${consumerRect.left + consumerRect.width / 2}px`;
      message.style.top = `${consumerRect.top + consumerRect.height / 2}px`;

      setTimeout(() => {
        message.remove();
        this.addMessageToConsumerArea(messageData);
      }, 800);
    }, 50);
  }

  addMessageToConsumerArea(messageData) {
    const consumedMessages = document.getElementById("consumedMessages");
    const message = document.createElement("div");
    message.className = "message consumed";
    message.textContent = messageData.Data
      ? messageData.Data.length > 20
        ? messageData.Data.substring(0, 20) + "..."
        : messageData.Data
      : `Msg-${messageData.Id}`;
    message.title = messageData.Data || messageData.Id;

    consumedMessages.appendChild(message);

    // Keep only last 10 messages visible
    while (consumedMessages.children.length > 10) {
      consumedMessages.removeChild(consumedMessages.firstChild);
    }
  }

  // Data Update Methods
  async updateQueues() {
    const queueNames = Array.from(this.queues.keys());

    // Get fresh queue data for all known queues
    for (const name of queueNames) {
      try {
        const response = await fetch(`${this.baseUrl}/queue/${name}`);
        if (response.ok) {
          const queueData = await response.json();
          this.queues.set(name, queueData);
        }

        const peekResponse = await fetch(`${this.baseUrl}/queue/${name}/peek`);
        if (peekResponse.ok) {
          const messages = await peekResponse.json();
          const queue = this.queues.get(name);
          if (queue) {
            queue.messages = messages || [];
            this.queues.set(name, queue);
          }
        }
      } catch (error) {
        console.error("Error updating queue:", name, error);
      }
    }

    this.render();
    this.updateSelectedQueueInfo();
    this.updateButtons();
  }

  startPolling() {
    setInterval(() => this.updateQueues(), 3000);
  }

  updateSystemStatus(message) {
    document.getElementById(
      "systemStatus"
    ).innerHTML = `${message}<br><small>${new Date().toLocaleTimeString()}</small>`;
  }

  render() {
    console.log("Render called. Queues size:", this.queues.size);
    const container = document.getElementById("queueContainer");

    if (this.queues.size === 0) {
      container.innerHTML =
        '<div class="no-queues">No queues created yet. Click "Create Queue" to start!</div>';
      return;
    }

    container.innerHTML = "";

    this.queues.forEach((queue, name) => {
      const queueElement = document.createElement("div");
      queueElement.className = "queue";
      queueElement.dataset.queueName = name;
      queueElement.onclick = () => this.selectQueue(name);

      const messageCount = queue.messages ? queue.messages.length : 0;
      const typeClass = queue.Type === 2 ? "priority" : "";

      queueElement.innerHTML = `
        <div class="queue-header">
          <div>
            <div class="queue-title">${name}</div>
            <div class="queue-stats">Messages: ${messageCount} | Created: ${new Date(
        queue.CreatedAt
      ).toLocaleDateString()}</div>
          </div>
          <div class="queue-type ${typeClass}">${this.getQueueTypeName(
        queue.Type
      )}</div>
        </div>
        <div class="messages">
          ${this.renderMessages(queue.messages || [], queue.Type)}
        </div>
      `;

      container.appendChild(queueElement);
    });

    // Restore selection
    if (this.selectedQueue) {
      const selectedElement = container.querySelector(
        `[data-queue-name="${this.selectedQueue.Name}"]`
      );
      if (selectedElement) {
        selectedElement.classList.add("selected");
      }
    }
  }

  renderMessages(messages, queueType) {
    if (!messages || messages.length === 0) {
      return '<div style="color: #6c757d; font-style: italic; text-align: center; width: 100%;">No messages</div>';
    }

    return messages
      .map((msg) => {
        const isPriority = queueType === 2 && msg.Priority && msg.Priority > 0;
        const isProcessing = msg.Status === 1; // MessageStatusProcessing
        const isConfirmed = this.confirmedMessages.has(msg.Id);
        const priorityBadge = isPriority
          ? `<div class="message-priority">${msg.Priority}</div>`
          : "";
        const confirmedBadge = isConfirmed
          ? `<div class="message-confirmed-badge">✓</div>`
          : "";

        let cssClass = "message";
        if (isPriority) cssClass += " priority";
        if (isProcessing) cssClass += " processing";
        if (isConfirmed) cssClass += " confirmed";

        const displayData =
          msg.Data && msg.Data.length > 30
            ? msg.Data.substring(0, 30) + "..."
            : msg.Data || `Msg-${msg.Id}`;

        return `
          <div class="${cssClass}" title="${msg.Data || msg.Id}${
          isConfirmed ? " (Confirmed)" : ""
        }">
            ${priorityBadge}
            ${confirmedBadge}
            ${displayData}
          </div>
        `;
      })
      .join("");
  }

  // Event Handlers for Modals
  handleModalClick(event, modalId) {
    if (event.target.id === modalId) {
      document.getElementById(modalId).style.display = "none";
    }
  }
}

// Initialize the visualizer
const visualizer = new BlitzQueueVisualizer();

// Modal click handlers
document.getElementById("createQueueModal").onclick = (e) => {
  if (e.target.id === "createQueueModal") {
    visualizer.hideCreateQueueModal();
  }
};

document.getElementById("sendMessageModal").onclick = (e) => {
  if (e.target.id === "sendMessageModal") {
    visualizer.hideSendMessageModal();
  }
};

// Override the sendMessage method to show modal
const originalSendMessage = visualizer.sendMessage;
visualizer.sendMessage = function () {
  this.showSendMessageModal();
};
