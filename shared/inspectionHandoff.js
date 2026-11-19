import {randomBytes} from 'node:crypto';
import {hostname, networkInterfaces} from 'node:os';

export const handoffNonce = () => randomBytes(32).toString('base64url');
export const validHandoffNonce = value => typeof value === 'string' && /^[A-Za-z0-9_-]{43}$/.test(value);
export const employeeKey = value => String(value ?? '').trim().replace(/\.0$/, '').replace(/^0+(?=\d)/, '');

export function isInspector(session) {
  const employee = session?.employee;
  const inspectionStation = /^line[12]inspection[12]$/.test(String(session?.machine_no || '').toLowerCase().replace(/[^a-z0-9]/g,''));
  return Boolean(employee && (inspectionStation || [employee.adp_job_title, employee.job_title]
    .some(title => /\binspector\b/i.test(String(title || '')))));
}

export function peerOrigin(req, application) {
  const configured = process.env[application === 'inspection' ? 'INSPECTION_PUBLIC_ORIGIN' : 'CNC_TIME_PUBLIC_ORIGIN'];
  if (configured) {
    const url = new URL(configured);
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password) throw Error('Invalid application address.');
    return url.origin;
  }
  const host = String(req.hostname || '').replace(/^\[|\]$/g, '').toLowerCase();
  const allowed = new Set(['localhost', '127.0.0.1', '::1', hostname().toLowerCase()]);
  for (const entries of Object.values(networkInterfaces())) for (const entry of entries || []) allowed.add(entry.address.toLowerCase());
  if (!allowed.has(host)) throw Error('Use the server’s normal sign-in address.');
  const secure = req.protocol === 'https';
  const port = application === 'inspection' ? (secure ? 3444 : 3000) : (secure ? 3443 : 3100);
  return `${secure ? 'https' : 'http'}://${host.includes(':') ? `[${host}]` : host}:${port}`;
}

export function isLoopback(req) {
  return ['127.0.0.1', '::1', '::ffff:127.0.0.1'].includes(req.socket?.remoteAddress);
}

export function createHandoffStore({now = Date.now, ttl = 60000} = {}) {
  const codes = new Map();
  return {
    issue(sessionId, user, state) {
      for (const [code, entry] of codes) if (entry.expires <= now()) codes.delete(code);
      if (codes.size >= 5000) throw Error('Please try signing in again shortly.');
      const code = handoffNonce();
      codes.set(code, {sessionId, state, employee:employeeKey(user.employee.emp_id), expires:now()+ttl,
        claims:{adp_number:user.employee.emp_id, machine_no:user.machine_no, shift_title:user.shift_title, shift_id:user.shift_id}});
      return code;
    },
    take(code, state) {
      const entry = codes.get(code);
      if (!entry || entry.expires <= now() || entry.state !== state) return null;
      codes.delete(code);
      return entry;
    },
  };
}
