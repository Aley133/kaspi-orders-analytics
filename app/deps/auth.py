# app/deps/auth.py


def _decode_hs256(token: str) -> Dict[str, Any]:
try:
return jwt.decode(
token,
SUPABASE_JWT_SECRET,
algorithms=["HS256"],
audience=JWT_AUDIENCE,
options={"verify_aud": bool(JWT_AUDIENCE), "verify_at_hash": False},
leeway=CLOCK_SKEW_SECONDS,
)
except JWTError:
raise HTTPException(401, "Invalid token")
except Exception:
raise HTTPException(401, "Invalid token")


def _decode_rs256_with_jwks(token: str) -> Dict[str, Any]:
if token.count(".") != 2:
raise HTTPException(401, "Invalid token")
try:
header = jwt.get_unverified_header(token)
kid = header.get("kid")
jwks = _get_jwks()
key = None
for k in jwks.get("keys", []):
if k.get("kid") == kid:
key = k
break
if not key:
keys = jwks.get("keys", [])
if keys:
key = keys[0]
if not key:
raise HTTPException(401, "Invalid token")
return jwt.decode(
token,
key,
algorithms=["RS256"],
audience=JWT_AUDIENCE,
options={"verify_aud": bool(JWT_AUDIENCE), "verify_at_hash": False},
leeway=CLOCK_SKEW_SECONDS,
)
except JWTError:
raise HTTPException(401, "Invalid token")
except Exception:
raise HTTPException(401, "Invalid token")


def get_current_user(req: Request) -> Dict[str, Any]:
token = _get_bearer_token(req)
if not token:
raise HTTPException(401, "Missing Bearer token")
if SUPABASE_JWT_SECRET:
claims = _decode_hs256(token)
else:
claims = _decode_rs256_with_jwks(token)
_ = claims.get("sub") or claims.get("user_id") or claims.get("uid") or claims.get("id")
return claims
