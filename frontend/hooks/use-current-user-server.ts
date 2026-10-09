import { auth } from "@/auth";

export const getCurrentUserServer = async () => {
  const session = await auth();

  return session?.user;
};