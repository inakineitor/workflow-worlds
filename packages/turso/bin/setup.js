#!/usr/bin/env node

// Simple wrapper to run the CLI from the bin directory
import('../dist/cli.js')
  .then((module) => {
    return module.setupDatabase();
  })
  .catch((err) => {
    console.error('Failed to load CLI:', err);
    process.exitCode = 1;
  });
