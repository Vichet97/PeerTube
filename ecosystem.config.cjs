module.exports = {
  apps: [
    {
      name: 'peertube',
      script: 'dist/server.js',
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
