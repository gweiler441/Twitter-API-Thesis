import { Actor } from 'apify';
import { ApifyClient } from 'apify-client';

const TWITTER_SCRAPER_ACTOR_ID = 'apidojo/twitter-scraper-lite';

await Actor.init();

try {
    // Get input from the Actor
    const input = await Actor.getInput();
    
    if (!input) throw new Error('No input provided');

    const {
        candidateElections = [], // Array of {candidate, electionYear, start, end}
        maxTweetsPerRun = 5,
        addUserInfo = true,
        scrapeTweetReplies = false,
    } = input;

    if (!candidateElections.length) throw new Error('No candidate elections provided');

    const client = new ApifyClient({ token: process.env.APIFY_TOKEN });
    const allTweets = [];

    console.log(`🚀 Starting Twitter Scraper Orchestrator`);
    console.log(`📊 Processing ${candidateElections.length} candidate-election combinations`);
    console.log(`Collecting up to ${maxTweetsPerRun} tweets per candidate per election\n`);

    let currentRun = 0;
    const totalRuns = candidateElections.length;

    // Iterate through each candidate-election combination
    for (const election of candidateElections) {
        currentRun++;
        
        console.log(`\n[Run ${currentRun}/${totalRuns}] Processing @${election.candidate} (${election.electionYear}: ${election.start} → ${election.end})`);

        // Build input for twitter-scraper-lite
        const searchQuery = `from:${election.candidate} since:${election.start} until:${election.end}`;
        
        const runInput = {
            searchTerms: [searchQuery],
            maxItems: maxTweetsPerRun * 4,
            sort: 'Latest',
            includeSearchTerms: false,
            addUserInfo,
        };

        try {
            // Launch the Twitter Scraper Lite actor
            const run = await client.actor(TWITTER_SCRAPER_ACTOR_ID).call(runInput);
            console.log(`  ✓ Run completed - Run ID: ${run.id}`);

            // Fetch dataset items
            const { items } = await client.dataset(run.defaultDatasetId).listItems();
            console.log(`  ✓ Retrieved ${items.length} raw tweets`);

            // Filter and sort tweets
            const startDate = new Date(election.start + 'T00:00:00Z');
            const endDate = new Date(election.end + 'T23:59:59Z');
            
            const filteredTweets = items
                .filter(item => {
                    const tweetDate = new Date(item.createdAt || item.created_at);
                    return tweetDate >= startDate && tweetDate <= endDate;
                })
                .sort((a, b) => {
                    const dateA = new Date(a.createdAt || a.created_at);
                    const dateB = new Date(b.createdAt || b.created_at);
                    return dateB - dateA;
                })
                .slice(0, maxTweetsPerRun);

            console.log(`  ✓ Filtered to ${filteredTweets.length} tweets within date range`);

            // Format and collect tweets
            for (const tweet of filteredTweets) {
                const tweetDate = new Date(tweet.createdAt || tweet.created_at);
                allTweets.push({
                    candidate: election.candidate,
                    electionYear: election.electionYear,
                    date: tweetDate.toISOString().split('T')[0],
                    text: tweet.text || tweet.full_text || '',
                    url: tweet.url || `https://twitter.com/${election.candidate}/status/${tweet.id_str || tweet.id || ''}`,
                });
            }

        } catch (error) {
            console.error(`  ✗ Error scraping @${election.candidate} for ${election.electionYear}: ${error.message}`);
        }

        // Small delay between runs
        if (currentRun < totalRuns) {
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    }

    // Push all tweets to dataset
    for (const tweet of allTweets) {
        await Actor.pushData(tweet);
    }

    // Summary
    console.log('\n' + '='.repeat(60));
    console.log('ORCHESTRATION SUMMARY');
    console.log('='.repeat(60));
    console.log(`Total tweets collected: ${allTweets.length}`);
    
    // Group by candidate
    const byCand = {};
    for (const tweet of allTweets) {
        if (!byCand[tweet.candidate]) byCand[tweet.candidate] = 0;
        byCand[tweet.candidate]++;
    }
    
    console.log('\nBreakdown by candidate:');
    for (const [candidate, count] of Object.entries(byCand)) {
        console.log(`  @${candidate}: ${count} tweets`);
    }
    
    // Group by election year
    const byYear = {};
    for (const tweet of allTweets) {
        if (!byYear[tweet.electionYear]) byYear[tweet.electionYear] = 0;
        byYear[tweet.electionYear]++;
    }
    
    console.log('\nBreakdown by election year:');
    for (const [year, count] of Object.entries(byYear).sort()) {
        console.log(`  ${year}: ${count} tweets`);
    }
    
    if (allTweets.length > 0) {
        console.log('\nDate range of collected tweets:');
        const dates = allTweets.map(t => t.date).sort();
        console.log(`  Earliest: ${dates[0]}`);
        console.log(`  Latest: ${dates[dates.length - 1]}`);
    }
    
    console.log('\n✅ Orchestration complete!');

} catch (error) {
    console.error('❌ Fatal error:', error.message);
    console.error(error);
    throw error;
}

await Actor.exit();
