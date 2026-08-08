const globals = require("globals");

module.exports = [
  {
    ignores: ["node_modules/**", "public/admin/tailwind.css", "logs/**"],
  },
  {
    files: ["src/**/*.js", "test/**/*.js", "scripts/**/*.js"],
    languageOptions: { ecmaVersion: 2023, sourceType: "commonjs", globals: { ...globals.node } },
    rules: {
      "no-undef": "error",
      "no-unused-vars": ["error", { argsIgnorePattern: "^_", caughtErrors: "none" }],
      "no-constant-condition": "error",
      "no-dupe-keys": "error",
      "no-unreachable": "error",
    },
  },
];
