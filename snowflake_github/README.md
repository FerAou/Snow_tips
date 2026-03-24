# Working with Snowflake and GitHub

A step-by-step guide to integrating Snowflake with GitHub repositories, enabling version control, collaboration, and CI/CD workflows directly from Snowflake.

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Authentication Methods](#authentication-methods)
4. [Step 1: Create a GitHub Personal Access Token](#step-1-create-a-github-personal-access-token)
5. [Step 2: Store Credentials in Snowflake](#step-2-store-credentials-in-snowflake)
6. [Step 3: Create an API Integration](#step-3-create-an-api-integration)
7. [Step 4: Create a Git Repository Clone](#step-4-create-a-git-repository-clone)
8. [Step 5: Work with Your Repository](#step-5-work-with-your-repository)
9. [Using Git in Snowflake Workspaces](#using-git-in-snowflake-workspaces)
10. [Troubleshooting](#troubleshooting)

---

## Overview

Snowflake natively integrates with Git repositories (GitHub, GitLab, Bitbucket), allowing you to:

- **Clone** remote repositories into Snowflake
- **Pull/Push** changes directly from Snowsight Workspaces
- **Reference** SQL scripts, notebooks, and Python files from Git
- **Automate** deployments with CI/CD pipelines

All interactions use HTTPS. The repository clone in Snowflake is a shallow copy that syncs with the remote on demand.

---

## Prerequisites

| Requirement | Details |
|---|---|
| Snowflake role | `ACCOUNTADMIN` or a role with `CREATE API INTEGRATION` and `CREATE GIT REPOSITORY` privileges |
| GitHub account | With access to the target repository |
| Repository URL | HTTPS format: `https://github.com/<owner>/<repo>.git` |

---

## Authentication Methods

Snowflake supports three authentication strategies:

| Method | Use Case | Setup Complexity |
|---|---|---|
| **No authentication** | Public repositories (read-only) | Low |
| **Personal Access Token (PAT)** | Private repos, scripted/CI access | Medium |
| **OAuth2 (Snowflake GitHub App)** | Workspace users, interactive workflows | Medium |

This guide covers the **PAT method**, which is the most common for private repositories.

---

## Step 1: Create a GitHub Personal Access Token

1. Go to **GitHub** > **Settings** > **Developer settings** > **Personal access tokens** > **Fine-grained tokens**
2. Click **Generate new token**
3. Configure:
   - **Token name**: `snowflake-integration`
   - **Expiration**: Choose based on your security policy
   - **Repository access**: Select the specific repositories
   - **Permissions**:
     - **Contents**: Read and Write (for push/pull)
     - **Metadata**: Read-only
4. Click **Generate token** and copy the value immediately

> **Important**: Store the token securely. You will not be able to see it again on GitHub.

---

## Step 2: Store Credentials in Snowflake

Create a Snowflake SECRET to securely store your GitHub credentials:

```sql
CREATE OR REPLACE SECRET my_git_secret
  TYPE = PASSWORD
  USERNAME = '<your_github_username>'
  PASSWORD = '<your_personal_access_token>';
```

**Example:**

```sql
CREATE OR REPLACE SECRET my_git_secret
  TYPE = PASSWORD
  USERNAME = 'FerAou'
  PASSWORD = 'ghp_xxxxxxxxxxxxxxxxxxxx';
```

> **Security**: Never commit tokens in plain text to files or repositories. Use Snowflake secrets to manage credentials securely.

---

## Step 3: Create an API Integration

The API integration tells Snowflake how to communicate with the GitHub API:

```sql
CREATE OR REPLACE API INTEGRATION my_git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/<your_github_account>')
  ALLOWED_AUTHENTICATION_SECRETS = (my_git_secret)
  ENABLED = TRUE;
```

**Example:**

```sql
CREATE OR REPLACE API INTEGRATION api_integration_skills
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/FerAou')
  ALLOWED_AUTHENTICATION_SECRETS = (my_git_secret)
  ENABLED = TRUE;
```

### Verify the integration:

```sql
SHOW INTEGRATIONS;
DESC INTEGRATION api_integration_skills;
```

Check that `API_ALLOWED_PREFIXES` matches your actual GitHub account URL.

---

## Step 4: Create a Git Repository Clone

Create a Snowflake object that clones your remote repository:

```sql
CREATE OR REPLACE GIT REPOSITORY my_db.my_schema.my_repo
  API_INTEGRATION = my_git_api_integration
  GIT_CREDENTIALS = my_git_secret
  ORIGIN = 'https://github.com/<owner>/<repo>.git';
```

**Example:**

```sql
CREATE OR REPLACE GIT REPOSITORY raw.public.snow_tips
  API_INTEGRATION = api_integration_skills
  GIT_CREDENTIALS = my_git_secret
  ORIGIN = 'https://github.com/FerAou/Snow_tips.git';
```

### Grant access to other roles (optional):

```sql
GRANT READ ON GIT REPOSITORY raw.public.snow_tips TO ROLE my_role;
```

---

## Step 5: Work with Your Repository

### Fetch latest changes from remote

```sql
ALTER GIT REPOSITORY raw.public.snow_tips FETCH;
```

### List branches and tags

```sql
SHOW GIT BRANCHES IN raw.public.snow_tips;
SHOW GIT TAGS IN raw.public.snow_tips;
```

### List files in a branch

```sql
LIST @raw.public.snow_tips/branches/main/;
```

### Execute a SQL script from the repository

```sql
EXECUTE IMMEDIATE FROM @raw.public.snow_tips/branches/main/scripts/my_script.sql;
```

---

## Using Git in Snowflake Workspaces

Snowflake Workspaces provide a native IDE experience with Git integration.

### Create a Workspace from a Git Repository

1. Go to **Projects** > **Workspaces** in Snowsight
2. Click **+ Add new**
3. A dialog appears: **"Create workspace from Git repository"** with three fields:

| Field | What to enter | Example |
|---|---|---|
| **Repository URL** | The HTTPS URL of your GitHub repo | `https://github.com/FerAou/Snow_tips.git` |
| **Workspace name** | A name for your local workspace | `Snow_tips` |
| **API integration** | Select an existing integration or click **+ API Integration** to create one | `api_integration_skills` |

4. Click **Create**

> **Note**: If you haven't created an API integration yet, click **+ API Integration** directly from this dialog. It will walk you through the setup (see [Step 3](#step-3-create-an-api-integration) above).

### Working in the Workspace

Once the workspace is created:

1. **Edit files** directly in the built-in editor (SQL, Python, YAML, Markdown)
2. **Pull** latest changes from the remote branch using the Git panel
3. **Push** your local changes back to the remote repository
4. **Switch branches** or create new ones from the branch selector in the top bar
5. **Run SQL files** directly from the workspace with the active Snowflake session

### OAuth2 Authentication (recommended for Workspaces)

For interactive Workspace usage, OAuth2 with the Snowflake GitHub App simplifies authentication:

```sql
CREATE OR REPLACE API INTEGRATION my_oauth_git_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com')
  API_USER_AUTHENTICATION = (TYPE = SNOWFLAKE_GITHUB_APP)
  ENABLED = TRUE;
```

Each user authenticates individually through the GitHub OAuth flow — no shared tokens needed.

---

## Troubleshooting

### Error: "Location is not allowed by integration"

**Cause**: The repository URL doesn't match `API_ALLOWED_PREFIXES`.

**Fix**: Update the integration with the correct GitHub account prefix:

```sql
CREATE OR REPLACE API INTEGRATION my_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/<correct_account>')
  ALLOWED_AUTHENTICATION_SECRETS = (my_git_secret)
  ENABLED = TRUE;
```

### Error: "Operation push is not permitted"

**Cause**: The Personal Access Token lacks write permissions.

**Fix**:
1. Generate a new PAT on GitHub with **Read and Write** access to **Contents**
2. Update the secret in Snowflake:

```sql
CREATE OR REPLACE SECRET my_git_secret
  TYPE = PASSWORD
  USERNAME = '<username>'
  PASSWORD = '<new_pat_with_write_access>';
```

### Error: "Failed to auth for unknown reason. HTTP: 404"

**Cause**: The Snowflake account identifier is incorrect.

**Fix**: Use the correct account identifier in format `orgname-accountname` (visible in your Snowsight URL).

### Error: "Failed to auth. HTTP: 401"

**Cause**: Invalid or expired credentials.

**Fix**:
1. Verify the PAT hasn't expired on GitHub
2. Regenerate the token if needed
3. Update the Snowflake secret with the new token

---

## Quick Reference

| Action | SQL Command |
|---|---|
| Create secret | `CREATE SECRET ... TYPE = PASSWORD` |
| Create API integration | `CREATE API INTEGRATION ... API_PROVIDER = git_https_api` |
| Create repo clone | `CREATE GIT REPOSITORY ... ORIGIN = '<url>'` |
| Sync from remote | `ALTER GIT REPOSITORY ... FETCH` |
| List branches | `SHOW GIT BRANCHES IN <repo>` |
| List files | `LIST @<repo>/branches/<branch>/` |
| Run SQL from Git | `EXECUTE IMMEDIATE FROM @<repo>/branches/<branch>/<file>` |
| Check integration | `DESC INTEGRATION <integration_name>` |

---

## Security Best Practices

- **Never hardcode tokens** in SQL files or repositories
- **Use Snowflake secrets** to store all credentials
- **Rotate PATs regularly** and set expiration dates
- **Use fine-grained tokens** scoped to specific repositories
- **Prefer OAuth2** for interactive Workspace usage
- **Grant least privilege** — only give `READ` or `WRITE` on Git repositories as needed
