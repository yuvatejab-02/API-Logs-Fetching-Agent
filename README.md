python send_test_message.py --incident-id AUTO_TEST_001 --service payment-service

docker-compose -f docker-compose.test.yml up --build


# Switch back to main
git checkout main
git pull origin main

# Merge with merge commit (preserves branch history)
git merge feature/major-component-update --no-ff

# Or use GitHub/GitLab interface for PR merge with merge commit


# Option 1: Revert the merge commit
git revert -m 1 <merge-commit-hash>

# Option 2: Reset to the tagged version
git reset --hard v1.0.0

# Option 3: Create a hotfix branch from the tag
git checkout -b hotfix/revert-major-changes v1.0.0





I can see it's using the OLD polling code (incident_poller) instead of the SQS mode. The task is running the default test payload with polling for 30 minutes and then exiting. Let me check what command the task is running:
I found the issue! The task definition doesn't have SQS_ENABLED=true set, and it has POLLING_DURATION_MINUTES=30, which means it's running in the old polling mode for 30 minutes and then exiting. Let me check all the environment variables: