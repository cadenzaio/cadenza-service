# Cadenza Service

## Overview

Cadenza is an innovative framework that extends traditional orchestration with event-driven choreography, providing "structured freedom" for building distributed, self-evolving systems.

The core package (@Cadenza.io/core) includes the foundational primitives for defining and executing graphs of tasks, managing contexts, handling signals, and bootstrapping executions. It's designed to be language-agnostic in model, with this TypeScript implementation serving as the reference.

Cadenza's design philosophy emphasizes:
- **Decentralized Adaptive Orchestration (DAO)**: Explicit graphs for dependencies (orchestration) combined with signals for loose coupling (choreography). This combination allows for flexible and dynamic workflows, while still maintaining the benefits of traditional orchestration.
- **Meta-Layer Extension**: A self-reflective layer for monitoring, optimization, and auto-generation, enabling AI-driven self-development. Essentially used for extending the core features by using the core features.
- **Introspectability**: Exportable graphs, traceable executions, and metadata separation for transparency and debugging.
- **Modularity**: Lightweight core with extensions (e.g., distribution, UI integration) as separate packages using the meta layer.

This repository (@Cadenza.io/service) is an extension of the core package, providing the infrastructure for making the Cadenza model truly distributed. It makes use of the core meta-layer extension, and provides a set of tools for building distributed applications, abstracting complexities like networking and security.

The service package provides everything in the core package plus the following:
- **Service Extension**: A Service exposes the local graphs and signals to other Services in the system and enables them to interact with tasks and signals across other Services in the system via socket and/or REST.
- **Database Service Extension**: A Database Service takes a schema and auto generates the necessary tasks and signals to interact with that database using the cadenza model. It exposes those tasks using by creating a Service.
- **CadenzaDB compatibility**: A service can connect to the official CadenzaDB service and will automatically sync realtime data for introspection and visualization.

There is no need to install the core package separately. Instead, install the service package, which includes everything in the core package plus the distributed extensions.

## Installation

Install the service package via npm:

```bash
npm install @cadenza.io/service
```

## Usage

### Creating a service
Creating a Service will create a REST server and a socket server. It will sync with the CadenzaDB service if available.

```typescript
import Cadenza from '@cadenza.io/service';

Cadenza.createCadenzaService('MyService'); // Name should start with an uppercase letter and contain no spaces.
```

### Working with distribution
Distribution is easy with Cadenza. You can delegate the flow by using a DeputyTask, or subscribe to foreign signals by prefixing them with the service name.

```typescript
// Creates a proxy Task for the specified task or routine on the specified service. 
// The local flow will await the result of the remote flow.
Cadenza.createDeputyTask('Process context', 'ContextService');

// This will subscribe to the signal on the remote service.
localTask.doOn('ContextService.process.failed');
```


For full examples, see this repository or the test suite.


## Features

## Architecture Overview
Cadenza's service package is divided into:


## Contributing
Contributions are welcome! Please fork the repo, create a branch, and submit a PR. Follow the code style and add tests for new features.

## License
MIT License

Copyright (c) 2025 Cadenza.io

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
