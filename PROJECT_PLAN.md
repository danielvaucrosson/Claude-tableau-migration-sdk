# Tableau Migration Tool - GUI Application Project Plan

## Goal
Build a C# WPF GUI application that **replicates and extends** the Tableau Content Migration Tool (CMT), powered by the Tableau Migration SDK.

---

## What We're Replicating from CMT

### Core Features to Match:

1. **6-Step Wizard Workflow**
   - Step 1: Sites (Source & Destination Configuration)
   - Step 2: Source Projects (Select what to migrate)
   - Step 3: Workbooks (Choose specific workbooks)
   - Step 4: Data Sources (Select published data sources)
   - Step 5: Permissions & Ownership (User/permission mapping)
   - Step 6: Options & Scripts (Pre/post migration scripts, settings)

2. **Migration Plan Management**
   - Create new migration plans
   - Save plans as reusable templates
   - Load existing plans
   - Edit and modify plans

3. **Execution & Monitoring**
   - Run migration with progress bar
   - Real-time status updates
   - Detailed logging output
   - Error handling and reporting

4. **User-Friendly GUI**
   - Clean, intuitive interface
   - Left sidebar navigation
   - Plan summary view before execution
   - Validation at each step

---

## What We're EXTENDING Beyond CMT

The Tableau Migration SDK gives us capabilities the CMT doesn't have:

### 1. Server-to-Cloud Migration
- **CMT Limitation**: Only supports Server-to-Server
- **Our Extension**: Full support for migrating from Tableau Server (on-prem) to Tableau Cloud

### 2. User & Group Migration
- **CMT Limitation**: Doesn't migrate users, groups, or site settings
- **Our Extension**: Migrate users, groups, and their memberships

### 3. Advanced Content Types
- **CMT Support**: Workbooks, data sources, projects (basic)
- **Our Extension**:
  - Extract refresh tasks
  - Server-level settings
  - More granular project hierarchies

### 4. SDK Power Features
- **Filters**: Exclude content based on rules (e.g., skip unlicensed users)
- **Mappings**: Transform data during migration (e.g., change email domains)
- **Transformers**: Modify content properties (e.g., add tags, update schedules)
- **Hooks**: Custom code execution at specific migration stages

### 5. Better Automation
- **Batch Operations**: Process migrations in configurable batch sizes
- **Retry Logic**: Automatic retry on failures
- **Incremental Migration**: Track what's been migrated, resume if interrupted

---

## Technical Architecture

### Technology Stack:

**Frontend (GUI):**
- **WPF** (Windows Presentation Foundation) - Modern Windows UI framework
- **MVVM Pattern** - Clean separation of UI and logic
- **MaterialDesignInXaml** - Modern, clean UI components (optional)

**Backend (Migration Logic):**
- **.NET 6+** (or latest LTS)
- **Tableau.Migration NuGet Package** - The official SDK
- **Dependency Injection** - For clean architecture (like SDK examples)
- **Async/Await** - For responsive UI during long migrations

**Data Persistence:**
- **JSON Files** - Store migration plans
- **SQLite** (optional) - For migration history/audit logs

---

## Project Structure

```
TableauMigrationTool/
├── TableauMigrationTool.sln          # Solution file
├── src/
│   ├── TableauMigrationTool.UI/      # WPF Application
│   │   ├── Views/                    # XAML UI files
│   │   │   ├── MainWindow.xaml
│   │   │   ├── Step1_Sites.xaml
│   │   │   ├── Step2_Projects.xaml
│   │   │   ├── Step3_Workbooks.xaml
│   │   │   ├── Step4_DataSources.xaml
│   │   │   ├── Step5_Permissions.xaml
│   │   │   └── Step6_Options.xaml
│   │   ├── ViewModels/               # MVVM ViewModels
│   │   │   ├── MainViewModel.cs
│   │   │   ├── SitesViewModel.cs
│   │   │   └── ...
│   │   └── App.xaml
│   │
│   ├── TableauMigrationTool.Core/    # Business Logic
│   │   ├── Models/                   # Data models
│   │   │   ├── MigrationPlan.cs
│   │   │   ├── SiteConfig.cs
│   │   │   └── ...
│   │   ├── Services/                 # Migration services
│   │   │   ├── MigrationService.cs
│   │   │   ├── PlanService.cs
│   │   │   └── TableauService.cs
│   │   └── Interfaces/               # Abstractions
│   │
│   └── TableauMigrationTool.SDK/     # SDK Integration
│       ├── Filters/                  # Custom filters
│       ├── Mappings/                 # Custom mappings
│       ├── Transformers/             # Custom transformers
│       └── Hooks/                    # Custom hooks
│
├── tests/                            # Unit tests
└── docs/                             # Documentation
```

---

## Development Phases

### Phase 1: Foundation (We are here)
- ✅ Research SDK and CMT
- ✅ Document architecture
- ⏳ Set up C# WPF project
- ⏳ Add Tableau Migration SDK NuGet package
- ⏳ Create basic project structure

### Phase 2: Core UI (Wizard)
- Create main window with navigation
- Build all 6 wizard steps (basic UI)
- Implement step-by-step navigation
- Add validation at each step

### Phase 3: SDK Integration
- Connect to Tableau Server/Cloud
- Fetch projects, workbooks, data sources
- Implement migration execution
- Add progress tracking

### Phase 4: Plan Management
- Save/load migration plans (JSON)
- Edit existing plans
- Plan templates
- Plan history

### Phase 5: Advanced Features
- Filters UI (exclude content)
- Mappings UI (transform data)
- Transformers UI (modify properties)
- Hooks/Scripts support

### Phase 6: Polish & Testing
- Error handling and validation
- Comprehensive logging
- User documentation
- Testing with real Tableau environments

---

## Key Differences: CMT vs Our Tool

| Feature | CMT | Our Tool (with SDK) |
|---------|-----|---------------------|
| **Migration Type** | Server ↔ Server only | Server ↔ Server AND Server → Cloud |
| **User Migration** | ❌ No | ✅ Yes |
| **Group Migration** | ❌ No | ✅ Yes |
| **Extract Refresh Tasks** | ❌ No | ✅ Yes |
| **Custom Filters** | Limited | ✅ Powerful SDK filters |
| **Data Transformations** | Basic mapping | ✅ Advanced mappings & transformers |
| **Custom Scripts** | Pre/post batch files | ✅ Plus SDK hooks (C# code) |
| **API** | REST API under the hood | ✅ SDK abstracts REST API complexity |
| **Content Types** | Workbooks, Data Sources | ✅ Plus Users, Groups, Tasks, etc. |
| **License** | Requires Advanced Management | ✅ Free & Open Source |

---

## Next Steps

1. **Set up the C# WPF project** with Tableau.Migration NuGet package
2. **Create a simple "Hello World" window** to verify setup
3. **Build Step 1 (Sites)** - Source/Destination configuration
4. **Test SDK connection** to a Tableau Server (or use mock data if no credentials)

---

## Questions to Consider

As we build this, you'll learn:
- **How WPF works** - XAML for UI, C# for logic
- **MVVM pattern** - How to structure GUI applications
- **Dependency Injection** - How modern apps manage dependencies
- **Async programming** - Keeping UI responsive during long operations
- **SDK concepts** - Filters, mappings, transformers, hooks

Don't worry if these sound complex - we'll go step by step!
