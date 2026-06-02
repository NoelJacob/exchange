<script>
  import UploadForm from '../lib/UploadForm.svelte';

  // Parse token from /submit/:token path
  let token = $derived(() => {
    const parts = window.location.pathname.split('/');
    // /submit/<token>  → index 2
    return parts.length >= 3 ? parts.slice(2).join('/') : '';
  });
</script>

<main class="submit-page">
  <h2>Submit Binary</h2>
  {#if token()}
    <UploadForm token={token()} />
  {:else}
    <p class="error">No token provided in URL.</p>
  {/if}
</main>

<style>
  .submit-page {
    max-width: 600px;
    margin: 0 auto;
  }
  .submit-page h2 {
    margin-bottom: 1rem;
  }
  .error { color: var(--danger); }
</style>
