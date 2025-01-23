const express = require("express");
const router = express.Router();
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const User = require("../models/User");

// JWT Secret from .env
const JWT_SECRET = process.env.JWT_SECRET || "secret123";

// User registration route (optional for testing)
router.post("/register", async (req, res) => {
    console.log("Request Body:", req.body); // Add this to debug
    const { username, password, role } = req.body;
    try {
      const hashedPassword = await bcrypt.hash(password, 10);
      const user = new User({ username, password: hashedPassword, role });
      await user.save();
      res.status(201).json({ message: "User registered successfully" });
    } catch (error) {
      res.status(500).json({ error: "Error creating user" });
    }
  });
// User login route
router.post("/login", async (req, res) => {
    const { username, password } = req.body;
    try {
      const user = await User.findOne({ username });
      if (!user) return res.status(404).json({ error: "User not found" });
  
      const isMatch = await bcrypt.compare(password, user.password);
      if (!isMatch) return res.status(401).json({ error: "Invalid credentials" });
  
      const token = jwt.sign({ id: user._id, role: user.role }, JWT_SECRET, { expiresIn: "1h" });
      res.status(200).json({ message: "Login successful", token, role: user.role });
    } catch (error) {
      res.status(500).json({ error: "Internal server error" });
    }
  });
module.exports = router;