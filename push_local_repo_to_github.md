# Pushing a Local Git Repo to a New GitHub Repo

How to push an existing local repo (with at least one commit) to GitHub when no GitHub repo exists yet.

## Option 1: GitHub CLI (one command)

Requires the [GitHub CLI](https://cli.github.com/) and a login (`gh auth login`). Run this from inside your repo:

```bash
gh repo create my-repo-name --private --source=. --remote=origin --push
```

- `--source=.` uses the current directory as the repo.
- `--remote=origin` adds the GitHub repo as your `origin` remote.
- `--push` pushes your commits right away.
- Use `--public` instead of `--private` for a public repo.

## Option 2: Create the repo on the website, then push

1. Go to [github.com/new](https://github.com/new).
2. Enter a repo name and choose public or private.
3. **Leave "Add a README", ".gitignore", and "license" unchecked.** Your local repo already has history, and adding those files would create a conflicting initial commit.
4. Click **Create repository**.
5. From inside your local repo, run:

```bash
git remote add origin https://github.com/YOUR-USERNAME/my-repo-name.git
git branch -M main
git push -u origin main
```

If you authenticate with SSH keys, use the SSH URL instead:

```bash
git remote add origin git@github.com:YOUR-USERNAME/my-repo-name.git
```

## Notes

- `git branch -M main` renames your current branch to `main`. Skip it if your branch already has the name you want, and push that branch name instead.
- `-u` sets upstream tracking, so later you can just run `git push` and `git pull`.
- If you get an auth error over HTTPS, GitHub no longer accepts account passwords. Use a [personal access token](https://github.com/settings/tokens), or let `gh auth login` or Git Credential Manager handle it.
- Check your remotes at any time with `git remote -v`.
