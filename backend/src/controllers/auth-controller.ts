import bcrypt from "bcryptjs";
import { json, type Request, type Response } from "express";
import { prisma } from "../db.js";
import { authSchema } from "../types/auth-schema.js";
import { createToken } from "../utils/auth.js";
import { sendValidationError } from "../utils/validation.js";

export async function signup(req: Request, res: Response): Promise<void> {
  const parsedBody = authSchema.safeParse(req.body);
  if (!parsedBody.success) {
    sendValidationError(res, parsedBody.error);
    return;
  }

  const { username, password } = parsedBody.data;
  const hashedPassword = await bcrypt.hash(password, 10);

  try {
    const user = await prisma.user.create({
      data: {
        username,
        password: hashedPassword,
      },
    });

    res.status(201).json({
      token: createToken({ userId: user.id }),
      userId: user.id,
      username: user.username,
    });
  } catch {
    res.status(409).json({ error: "username already exists" });
  }
}

export async function signin(req: Request, res: Response): Promise<void> {
  const parsedBody = authSchema.safeParse(req.body);

  if (!parsedBody.success) {
    sendValidationError(res, parsedBody.error);
    return;
  }

  const { username, password } = parsedBody.data;

  try {
    const existingUser = await prisma.user.findUnique({
      where: {
        username: username,
      },
      select: {
        id: true,
        username: true,
        password: true,
      },
    });

    if (!existingUser) {
      res.status(404).json({
        message: "user not found, try signing-up",
      });
      return;
    }

    const passwordMatches = await bcrypt.compare(
      password,
      existingUser.password,
    );

    if (!passwordMatches) {
      res.status(401).json({
        message: "invalid credentials",
      });
      return;
    }

    res.status(200).json({
      message: "signin successfull",
      token: createToken({
        userId: existingUser.id,
      }),
    });
  } catch {
    res.status(500).json({
      message: "internal server error",
    });
  }
}
