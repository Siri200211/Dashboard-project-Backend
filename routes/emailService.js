const nodemailer = require("nodemailer");

const getEmailTransporter = (service) => {
  let transporterConfig;

  if (service === "gmail") {
    transporterConfig = {
      host: "smtp.gmail.com",
      port: 587,
      secure: false, // use TLS
      auth: {
        user: process.env.GMAIL_USER,
        pass: process.env.GMAIL_PASS,
      },
    };
  } else if (service === "outlook") {
    transporterConfig = {
      host: process.env.SMTP_OUTLOOK_HOST,
      port: Number(process.env.SMTP_OUTLOOK_PORT),
      secure: false, // use TLS
      auth: {
        user: process.env.OUTLOOK_USER,
        pass: process.env.OUTLOOK_PASS,
      },
    };
  } else {
    throw new Error("Invalid email service selected");
  }

  return nodemailer.createTransport(transporterConfig);
};

// Function to send email
const sendEmail = async ({ service, to, subject, text, html }) => {
  try {
    const transporter = getEmailTransporter(service);
    const mailOptions = {
      from:
        service === "gmail"
          ? process.env.GMAIL_USER
          : process.env.OUTLOOK_USER,
      to,
      subject,
      text,
      html,
    };

    const info = await transporter.sendMail(mailOptions);
    console.log(`Email sent: ${info.messageId}`);
    return info;
  } catch (error) {
    console.error("Error sending email:", error.message);
    throw new Error("Failed to send email");
  }
};

module.exports = { sendEmail };