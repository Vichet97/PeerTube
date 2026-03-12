module.exports = {
  apps: [
    {
      name: 'peertube',
      script: 'dist/server',
      interpreter: 'node',
      cwd: __dirname,
      autorestart: true,
      restart_delay: 10000,
      watch: false,
      env: {
        NODE_ENV: 'production'
      }
    }
  ]
}
