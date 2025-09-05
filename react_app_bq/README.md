# BlitzQueue React Visualizer

Uma versão React simples do BlitzQueue Visualizer que replica exatamente a funcionalidade do visualizer.js original.

## 🎯 Princípio KISS (Keep It Simple, Stupid)

Esta implementação segue o princípio KISS:

- **Um único componente** que gerencia todo o estado
- **Hooks simples** (useState, useEffect, useCallback)
- **CSS idêntico** ao original
- **Funcionalidades exatas** do visualizer.js

## 🚀 Como usar

### 1. Instalar dependências

```bash
cd react_app_bq
npm install
```

### 2. Iniciar o servidor BlitzQueue

```bash
# Na pasta raiz do projeto
make run-queue
# ou
set CGO_ENABLED=1 && go run cmd/blitzqueue.go
```

### 3. Iniciar o React

```bash
cd react_app_bq
npm start
```

O React vai abrir em http://localhost:3000 e se conectar com o backend em http://localhost:52525

## 📋 Funcionalidades

✅ **Carregar filas existentes** - Ao inicializar
✅ **Criar filas** - Standard, FIFO, Priority, Scheduled  
✅ **Enviar mensagens** - Com campos específicos por tipo de fila
✅ **Visualizar mensagens** - Peek sem consumir
✅ **Consumir mensagens** - Move para consumer
✅ **Confirmar mensagens** - Muda cor para azul
✅ **Seleção de fila** - Interface reativa
✅ **Status em tempo real** - Polling a cada 3 segundos
✅ **Modais responsivos** - Igual ao original
✅ **Animações CSS** - Todas preservadas

## 🎨 Diferenças visuais

**ZERO!** O React usa o mesmo CSS e produz o resultado visual idêntico.

## 🏗️ Estrutura

```
react_app_bq/
├── package.json         # Dependências mínimas
├── public/
│   └── index.html      # HTML básico
└── src/
    ├── index.js        # Inicialização React
    ├── App.js          # Componente principal
    └── App.css         # CSS idêntico ao original
```

## 🔄 Estado gerenciado

- `queues` - Map com todas as filas
- `selectedQueue` - Fila atualmente selecionada
- `consumedMessages` - Mensagens consumidas
- `pendingConfirmations` - IDs aguardando confirmação
- `confirmedMessages` - Set de mensagens confirmadas
- `systemStatus` - Status atual do sistema
- Estados dos modais e formulários

Simples, direto e funcional! 🎯
