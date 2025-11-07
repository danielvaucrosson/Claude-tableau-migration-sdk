# Tableau Migration SDK - Overview & Getting Started

## What is the Tableau Migration SDK?

The Tableau Migration SDK is a **client library** (not an API) that helps you build applications to migrate content from **Tableau Server** (on-premises) to **Tableau Cloud** (hosted).

### Key Difference: SDK vs API
- **API**: You make HTTP requests to endpoints and get responses back
- **SDK**: You import a library into your code and call methods/classes that handle the complex work for you

Think of it like this: Instead of manually making HTTP calls to Tableau's REST API, the SDK gives you pre-built functions that do all the heavy lifting.

---

## What Can It Do?

The SDK migrates these content types:
1. **Users** - User accounts and their settings
2. **Groups** - User groups and memberships
3. **Projects** - Project hierarchies and organization
4. **Data Sources** - Published data sources
5. **Workbooks** - Tableau workbooks with all their sheets
6. **Extract Refresh Tasks** - Scheduled refresh tasks

---

## How Does It Work?

The migration process follows this pipeline:

```
SOURCE (Tableau Server)
    ↓
  FILTERS (exclude content you don't want)
    ↓
  MAPPINGS (transform data during migration)
    ↓
  TRANSFORMERS (modify content properties)
    ↓
  HOOKS (run custom code at specific stages)
    ↓
DESTINATION (Tableau Cloud)
```

### Core Concepts:

1. **Filters**: Decide what to migrate (e.g., skip default projects)
2. **Mappings**: Change values during migration (e.g., update email domains)
3. **Transformers**: Modify content (e.g., add tags, change permissions)
4. **Hooks**: Execute custom logic at specific points in the migration

---

## Language Options

You can use either:
- **C#** (.NET) - Primary language, most examples
- **Python** - Wrapper around the .NET library

---

## What You Need to Get Started

### 1. Prerequisites
- **.NET Runtime** (required even for Python)
- **Access credentials** for:
  - Source: Tableau Server (on-premises)
  - Destination: Tableau Cloud

### 2. Credentials Format
You'll need **Personal Access Tokens** (not passwords) for both:
- Token Name (identifier)
- Token Secret (the actual credential)

### 3. Configuration
The SDK uses a configuration file (appsettings.json) with:
- Source server URL and site
- Destination server URL and site
- Access tokens
- Migration settings (batch sizes, content types, etc.)

---

## Example Configuration Structure

```json
{
  "tableau": {
    "migrationSdk": {
      "source": {
        "serverUrl": "http://your-tableau-server",
        "siteContentUrl": "",
        "accessTokenName": "my-server-token-name"
      },
      "destination": {
        "serverUrl": "https://pod.online.tableau.com",
        "siteContentUrl": "your-site-name",
        "accessTokenName": "my-cloud-token-name"
      }
    }
  }
}
```

---

## Next Steps - What We Can Do

### Option 1: Learn by Example (Recommended for beginners)
- Set up a simple example project
- Walk through the code step-by-step
- Explain each concept as we go
- Use mock/test credentials to understand the flow

### Option 2: Build a Basic Migration Tool
- Create a minimal working migration application
- Configure it for your specific environment
- Test with a small subset of content

### Option 3: Explore Specific Features
- Deep dive into filters, mappings, or transformers
- Understand how to customize the migration process
- Learn about error handling and logging

---

## Questions for You

Before we proceed, I'd like to know:

1. **Do you have access to a Tableau Server and Tableau Cloud instance?**
   - If yes: Do you have Personal Access Tokens for both?
   - If no: We can still explore the SDK with example/mock setups

2. **Language preference?**
   - C# (more examples, better documented)
   - Python (more familiar to many developers)

3. **What's your goal?**
   - Just understand how SDKs work in general?
   - Actually migrate content between Tableau environments?
   - Learn the SDK architecture and capabilities?

4. **Learning pace?**
   - Start with absolute basics (What is dependency injection? How do .NET projects work?)
   - Jump into SDK specifics (assume you know .NET/Python basics)
