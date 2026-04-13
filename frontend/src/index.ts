import express from "express";
import path from "path";
import { createProxyMiddleware } from "http-proxy-middleware";

const app = express();
const PORT = 3000;

app.use(
  "/api",
  createProxyMiddleware({
    target: "http://localhost:8000",
    changeOrigin: true,
  })
);

app.use(express.static(path.join(__dirname, "public")));

app.listen(PORT, () => {
  console.log(`VitalSync dashboard running at http://localhost:${PORT}`);
});
