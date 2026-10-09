# SecureShare frontend (Next.js)

Configure via `.env.local` (see `.env.example`): `API_BASE_URL` (server-side) and `NEXT_PUBLIC_API_URL` (browser)
must point at the load balancer, e.g. `http://localhost:8080/api`.

    npm install && npm run dev      # http://localhost:3000

New page: `/cluster` shows live storage-node health, leader, replica statistics and which API node answered.
