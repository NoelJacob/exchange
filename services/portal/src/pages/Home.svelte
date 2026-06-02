<script>
  import { connectLeaderboard } from '../lib/ws.js';
  import Leaderboard from '../lib/Leaderboard.svelte';
  import LiveChart from '../lib/LiveChart.svelte';

  let leaderboardData = $state([]);
  let historyData = $state([]);
  let selectedContestant = $state(null);

  $effect(() => {
    const cleanup = connectLeaderboard((snapshot) => {
      const contestants = snapshot.contestants ?? snapshot.leaderboard ?? snapshot.data ?? [];
      leaderboardData = contestants;

      // Append to history (keep last 60 snapshots)
      historyData = [
        ...historyData.slice(-59),
        { timestamp: Date.now(), contestants },
      ];

      // Preserve selection if still in data
      if (selectedContestant) {
        const stillThere = contestants.find(c => c.name === selectedContestant.name);
        if (!stillThere) {
          selectedContestant = null;
        }
      }
    });

    return cleanup;
  });

  function handleSelect(row) {
    selectedContestant = row;
  }
</script>

<Leaderboard data={leaderboardData} onselect={handleSelect} />

{#if selectedContestant}
  <LiveChart
    selectedContestant={selectedContestant}
    historyData={historyData}
    contestants={leaderboardData}
  />
{/if}
