const express = require("express");
const fetch = require("node-fetch");

const app = express();
const PORT = 7900;

// Fetch & store
app.get("/health/:id", async (req, res) => {
  const { id } = req.params;

  try {
    const r = await fetch(
      `http://backend-service:7901/status/${id}`
    );
    const data = await r.json();
    res.status(r.status).json(data);
  } catch (err) {
    res.status(500).json({ error: "Backend unreachable" });
  }
});

// Read all data
app.get("/todos", async (req, res) => {
  try {
    const r = await fetch("http://backend-service:7901/todos");
    const data = await r.json();
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: "Backend unreachable" });
  }
});

app.listen(PORT, "0.0.0.0", () =>
  console.log(`API running on ${PORT}`)
);
