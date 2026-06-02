<script>
  import Home from './pages/Home.svelte';
  import Submit from './pages/Submit.svelte';
  import Admin from './pages/Admin.svelte';

  let route = $state(window.location.pathname);

  $effect(() => {
    function onPop() {
      route = window.location.pathname;
    }
    window.addEventListener('popstate', onPop);
    return () => window.removeEventListener('popstate', onPop);
  });

  function navigate(e) {
    const href = e.currentTarget.getAttribute('href');
    if (!href) return;
    e.preventDefault();
    history.pushState(null, '', href);
    route = href;
  }
</script>

<nav>
  <h1>IICPC Benchmark</h1>
  <a href="/" onclick={navigate}>Leaderboard</a>
  <a href="/admin" onclick={navigate}>Admin</a>
</nav>

<main>
  {#if route === '/'}
    <Home />
  {:else if route.startsWith('/submit')}
    <Submit />
  {:else if route === '/admin'}
    <Admin />
  {:else}
    <p>Page not found</p>
  {/if}
</main>
