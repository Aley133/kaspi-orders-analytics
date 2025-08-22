(function(){
  const key = "theme";
  const root = document.body;
  const btn = document.getElementById("theme-toggle");

  function setTheme(v){
    root.classList.remove("theme-light","theme-dark");
    root.classList.add(v);
    localStorage.setItem(key,v);
  }

  const saved = localStorage.getItem(key) || "theme-light";
  setTheme(saved);

  btn.addEventListener("click", (e)=>{
    e.preventDefault();
    const now = root.classList.contains("theme-dark") ? "theme-light" : "theme-dark";
    setTheme(now);
  });
})();
