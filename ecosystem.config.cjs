const python = process.platform === "win32" ? "python" : "python3";

module.exports = {
  apps: [
    {
      name: "pump-scan",
      cwd: ".",
      script: "scan.py",
      interpreter: python,
      instances: 1,
      autorestart: true,
      max_restarts: 50,
      restart_delay: 5000,
      stop_exit_codes: "0 130",
      time: true,
      merge_logs: true,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      out_file: "logs/scan.out.log",
      error_file: "logs/scan.err.log",
    },
    {
      name: "pump-fetch",
      cwd: ".",
      script: "fetch.py",
      interpreter: python,
      instances: 1,
      autorestart: true,
      max_restarts: 50,
      restart_delay: 5000,
      stop_exit_codes: "0 130",
      time: true,
      merge_logs: true,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      out_file: "logs/fetch.out.log",
      error_file: "logs/fetch.err.log",
    },
  ],
};
