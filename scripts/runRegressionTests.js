import { runRegressionSuite } from "../src/regression/runRegressionSuite.js";

const result = runRegressionSuite();

if (result?.fail > 0) {
  process.exit(1);
}
