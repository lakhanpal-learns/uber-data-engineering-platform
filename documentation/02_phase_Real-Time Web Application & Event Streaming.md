# Phase 02 — Build Web Application and Connect to Event Hub

## 1. Overview

In this phase, we build the application that acts as a **producer** and sends ride events to Azure Event Hub.

The basic workflow is:

```text
Web Application
       │
       │ Generate Ride Events
       ▼
   Azure Event Hub
       │
       │ Streaming Data
       ▼
    Databricks
```

The application uses the **Event Hub connection string** and **Event Hub name** to connect to the Event Hub.

### Reference

[Microsoft Azure Event Hubs — Send events using Python](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-python-get-started-send?tabs=passwordless%2Croles-azure-portal)

---

# 2. Event Hub Connection Details

The application requires two important configuration values:

- **Connection String**
- **Event Hub Name**

These values are obtained from the Azure Event Hub configuration.

```text
Azure Event Hub
      │
      ├── Connection String
      │
      └── Event Hub Name
               │
               ▼
          Web Application
```

> **Important:** Never commit the connection string to GitHub. Store it in an environment file or another secure secret-management system.

---

# 3. Step 1 — Initialize the Project

The project is initialized using **uv**, a modern Python project and package management approach.

The project environment is created using a virtual environment.

```text
Python Project
      │
      ▼
     uv
      │
      ▼
Virtual Environment
      │
      ▼
Application
```

---

# 4. Step 2 — Clone the Repository

Clone the project repository:

```text
https://github.com/anshlambagit/Uber_Data_Engineer_Project
```

After cloning, move into the project directory and continue with the setup.

---

# 5. Step 3 — Create the Environment File

Create a `.env` file in the project.

The `.env` file stores the Event Hub configuration required by the application.

Example:

```env
EVENT_HUB_CONNECTION_STRING="your_connection_string"
EVENT_HUB_NAME="your_event_hub_name"
```

The values come from the Azure Event Hub configuration.

```text
Azure Event Hub
      │
      ├── Connection String
      │
      └── Event Hub Name
               │
               ▼
             .env
               │
               ▼
        Python Application
```

> **Important:** Add `.env` to `.gitignore` so that the connection string is not pushed to GitHub.

---

# 6. Step 4 — Load Environment Variables

The application uses `load_dotenv` to load the variables stored in the `.env` file into the Python environment.

The basic flow is:

```text
.env
 │
 │ load_dotenv()
 ▼
Python Environment
 │
 ▼
Python Application
```

This allows the application to access the Event Hub connection details without hardcoding them directly into the Python source code.

---

# 7. Step 5 — Build the Event Hub Producer

The application code can be understood as two main parts.

```text
Main Application
       │
       ├── 1. Generate Data
       │
       └── 2. Send Data to Event Hub
```

## 7.1 Event Hub Connection Function

A function is responsible for creating the connection to Azure Event Hub.

```text
Connection Function
        │
        ├── Connection String
        │
        └── Event Hub Name
                │
                ▼
           Event Hub Client
```

The connection function handles the Event Hub client/connection that will be used to send events.

---

## 7.2 Main Function

The main function coordinates the overall process.

It can be divided into two major steps:

### Step 1 — Generate Data

The application generates ride-related data/events.

```text
Generate Ride Data
        │
        ▼
     Event Data
```

### Step 2 — Send Data

The generated event is sent to Event Hub using the Event Hub connection.

```text
Generated Event
       │
       ▼
Event Hub Connection
       │
       ▼
Azure Event Hub
```

The complete flow is:

```text
              MAIN FUNCTION
                    │
          ┌─────────┴─────────┐
          │                   │
          ▼                   ▼
   Generate Data         Event Hub Connection
          │                   │
          └─────────┬─────────┘
                    │
                    ▼
              Send Event
                    │
                    ▼
               Event Hub
```

---

# 8. Step 6 — Verify the Event Hub

After running the producer application, verify that events are reaching the Event Hub.

Open the Event Hub in the Azure Portal.

Use the **Data Explorer** option to inspect the incoming event data.

```text
Python Application
       │
       │ Send Events
       ▼
   Azure Event Hub
       │
       ▼
   Data Explorer
       │
       ▼
  Incoming Events
```

If the producer is working correctly, the generated events should appear in the Event Hub.

---

# 9. Step 7 — Optional Web Application

The producer can optionally be exposed through a web application.

Instead of running the producer directly from a Python script, the web application can trigger the event-generation process.

The architecture becomes:

```text
             WEB APPLICATION
                    │
                    │ User Action
                    ▼
              Python API
                    │
                    │ Generate Event
                    ▼
              Azure Event Hub
                    │
                    ▼
                Databricks
```

For example, the application can be run using **Uvicorn**:

```bash
uvicorn api:app --reload
```

---

# 10. Virtual Environment Commands

Activate the Python virtual environment:

```powershell
.venv\Scripts\Activate.ps1
```

Deactivate the virtual environment:

```bash
deactivate
```

---

# 11. Complete Phase Workflow

The complete workflow implemented in this phase is:

```text
                   WEB APPLICATION
                          │
                          ▼
                   Generate Event
                          │
                          ▼
                   Event Hub Client
                          │
                          │ Connection String
                          ▼
                    Azure Event Hub
                          │
                          ▼
                    Data Explorer
                          │
                          ▼
                       Databricks
```

---

# 12. Phase 02 Result

At the end of this phase:

- The Python project is initialized.
- The Event Hub connection details are stored in `.env`.
- The application can load environment variables.
- The application generates ride events.
- The application sends events to Azure Event Hub.
- Event Hub Data Explorer can be used to verify incoming events.
- An optional web API can be used to trigger event generation.

The resulting real-time flow is:

```text
Web Application
       │
       ▼
Generate Ride Event
       │
       ▼
Azure Event Hub
       │
       ▼
Databricks
```

# uv appproch 
uv manages the dependency tree underneath them.

example 
uv add azure-eventhub
          ↓
       uv checks
          ↓
azure-eventhub requires other packages
          ↓
uv finds compatible versions
          ↓
installs them all