https://github.com/jexniemi/browserless-action-runner
name: Scrape web

on: [push]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - name: Checkout code
      uses: actions/checkout@v3

    - name: Set up Node.js
      uses: actions/setup-node@v4
      with:
        node-version: "18"

    - name: Install dependencies
      run: cd example && npm install

    - name: Compile TypeScript
      run: cd example && npx tsc

    - name: Run Puppeteer Action
      uses: jexniemi/browserless-action-runner@v0.1
      with:
        start_commands: | 
          node example/build/index.js
