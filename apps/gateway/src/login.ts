export const loginPage = `<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>DSH Cloud</title><style>
:root{font-family:Inter,ui-sans-serif,system-ui,-apple-system,sans-serif;color:#171717;background:#f5f5f2}*{box-sizing:border-box}
body{margin:0;min-height:100vh;display:grid;place-items:center;background:radial-gradient(circle at 20% 10%,#fff 0,#f5f5f2 45%,#ecece6 100%)}
main{width:min(420px,calc(100vw - 32px));background:#fff;border:1px solid #deded8;border-radius:18px;padding:32px;box-shadow:0 18px 70px #2221}
h1{font-size:25px;margin:0 0 8px}.hint{color:#696963;margin:0 0 24px}.tabs{display:flex;gap:8px;margin-bottom:18px}
button,input{font:inherit}button.tab{border:0;background:#efefe9;padding:8px 14px;border-radius:9px;cursor:pointer}.tab.active{background:#1e1e1c;color:#fff}
form{display:grid;gap:13px}label{font-size:13px;color:#555}input{width:100%;margin-top:5px;padding:11px 12px;border:1px solid #cdcdc7;border-radius:9px;outline:none}input:focus{border-color:#555}
button.submit{margin-top:5px;padding:12px;border:0;border-radius:10px;background:#1f1f1d;color:#fff;cursor:pointer}.error{min-height:20px;color:#a22;font-size:13px}
</style></head><body><main><h1>DSH Cloud</h1><p class="hint">登录后进入你的隔离 Coding Agent 工作区。</p>
<div class="tabs"><button class="tab active" data-mode="login">登录</button><button class="tab" data-mode="register">注册</button></div>
<form id="auth"><label id="name-label" hidden>团队名称<input name="name" autocomplete="organization"></label><label>邮箱<input name="email" type="email" required autocomplete="email"></label><label>密码<input name="password" type="password" minlength="10" required autocomplete="current-password"></label><div class="error" id="error"></div><button class="submit">登录</button></form>
<script>let mode='login';const form=document.querySelector('#auth'),err=document.querySelector('#error'),nameLabel=document.querySelector('#name-label'),submit=document.querySelector('.submit');document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>{mode=b.dataset.mode;document.querySelectorAll('.tab').forEach(x=>x.classList.toggle('active',x===b));nameLabel.hidden=mode==='login';submit.textContent=mode==='login'?'登录':'创建账户';err.textContent=''});form.onsubmit=async e=>{e.preventDefault();err.textContent='';const body=Object.fromEntries(new FormData(form));const r=await fetch('/cloud/'+mode,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});if(r.ok)location.href='/';else{const j=await r.json().catch(()=>({}));err.textContent=j.error||'请求失败'}};</script></main></body></html>`
