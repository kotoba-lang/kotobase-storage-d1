/**
 * Externs for the Cloudflare Workers runtime objects this Worker receives from
 * the host rather than constructs itself.
 *
 * Property names listed here are excluded from :advanced renaming. Without
 * this, `env.DB` compiles to `env.Eb` and every D1 call becomes
 * `undefined.prepare` -- a 500 on every route, with no compile-time error.
 * A `^js` hint alone did not prevent the rename here (measured), and `aget` /
 * goog.object/get with a literal key silence the infer warning while keeping
 * the rename, so the declaration is made explicit.
 */

/** @type {!Object} */
var __cloudflare_env__;

/** @type {!Object} */
__cloudflare_env__.DB;
