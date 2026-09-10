# 02 — Git & GitHub from absolute zero

## What are these two things?

- **git** = a program on YOUR laptop that remembers every version of your files.
  Like "infinite undo" plus a labelled history of milestones (*commits*).
- **GitHub** = a website that stores your git history online so it's backed up,
  shareable, and can trigger automation (our CI checks).

git works offline. GitHub is just where you *push* a copy.

## The five commands you will use 95% of the time

```bash
git status                 # "what has changed since my last commit?"
git add .                  # "stage all my changes for the next commit"
git commit -m "message"    # "take a snapshot with this label"
git push                   # "upload my commits to GitHub"
git pull                   # "download commits from GitHub (if any)"
```

## First time: putting THIS repo on GitHub

Open a terminal **inside the `project1_fraud` folder** and run:

```bash
# 1) turn the folder into a git repository (creates a hidden .git folder)
git init
git branch -M main

# 2) make your first commit (all files, since none are tracked yet)
git add .
git commit -m "chore: initial project structure for fraud lakehouse"
```

Now create the online home:

1. Go to https://github.com and sign up / log in (free).
2. Top-right **+ → New repository**.
3. Name it `fraud-lakehouse` (or anything), keep it **Public** (or Private, your call).
4. Do NOT tick "Add a README" (we already have one) → **Create repository**.
5. GitHub shows you a page with commands — the middle block is what we need.
   Copy YOUR version of these two lines (it contains your username):

```bash
git remote add origin https://github.com/<you>/fraud-lakehouse.git
git push -u origin main
```

6. Refresh the GitHub page — your files are online. The CI workflow
   (`.github/workflows/ci.yml`) will run automatically on every push;
   you can watch it in the **Actions** tab.

> If `git push` asks for a password: GitHub no longer accepts account passwords
> from the terminal. Click "Sign in with browser" if you use Git Credential
> Manager (installed with Git for Windows / macOS), or create a
> **Personal Access Token** (Settings → Developer settings → Tokens) and paste
> it as the password.

## Daily workflow (do this after every work session)

```bash
git status                       # see what changed
git add .                        # stage everything
git commit -m "feat: add NiFi transaction generator flow"
git push                         # back it up
```

Good commit prefixes (a convention called *Conventional Commits*):
`feat:` new thing · `fix:` repair · `docs:` docs only · `chore:` housekeeping.

## What will NOT be uploaded (thanks to `.gitignore`)

- `.env` — your local passwords. Only `.env.example` (the empty template) is shared.
- Python caches, Airflow logs, OS junk files.

## Getting the repo onto a fresh machine later

```bash
git clone https://github.com/<you>/fraud-lakehouse.git
cd fraud-lakehouse
cp .env.example .env
make up
```

That's the whole point: **one `git clone` + one `make up` rebuilds the world.**
