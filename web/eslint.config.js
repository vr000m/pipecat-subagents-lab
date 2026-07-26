const browserGlobals = Object.fromEntries([
  "MediaStream",
  "URL",
  "alert",
  "console",
  "document",
  "globalThis",
  "location",
  "setTimeout",
  "window",
].map((name) => [name, "readonly"]));

const testGlobals = Object.fromEntries(["expect", "test"].map((name) => [name, "readonly"]));

export default [
  {
    files: ["src/**/*.js", "test/**/*.js"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: { ...browserGlobals, ...testGlobals },
    },
    rules: {
      "no-undef": "error",
    },
  },
];
