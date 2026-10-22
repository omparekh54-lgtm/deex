Supabase's public production root CA, downloaded on 2026-10-19 from the URL
specified by the official dashboard source:

https://github.com/supabase/supabase/blob/master/apps/studio/hooks/custom-content/custom-content.json

Download: https://supabase-downloads.s3-ap-southeast-1.amazonaws.com/prod/ssl/prod-ca-2021.crt

DER SHA-256: `807025ad50d4ed219d2c9c7d299c004f824eb00cf7f65afef607d07b72e6cafa`

Subject/issuer: Supabase Root 2021 CA. Valid from 2021-04-28 through 2031-04-26.
This is a public trust anchor, not a private key or credential. It is selected
only for Supabase database/pooler hostnames when cloud verify-full policy is
enabled. Explicit reviewed CA configuration takes precedence. Other providers
use the system trust store. Certificate and hostname verification stay enabled;
no peer certificate is trusted merely because it was received over the network.
Provider certificate rotation requires a reviewed update and a deployed readiness
check. No certificate is downloaded dynamically at startup.
