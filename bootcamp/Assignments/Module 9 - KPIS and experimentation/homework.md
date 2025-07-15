# Homework

- Pick a product that you love using (spotify, linkedin, etc)
  - Describe the user journey of the things you loved about it from the moment you started using it to your use of it now
  - Describe 3 experiments you would like to run on this product to see if it would improve the experience
    - You should detail out the allocation of each test cell and the different conditions you’re testing
    - Your hypothesis of which leading and lagging metrics would be impacted by this experiment
  
- Put these files in Markdown files and submit [here](https://bootcamp.techcreator.io/assignments)
fasdjeinasdflkjfasdf

## Youtube

I was a kid when YouTube was launched. Back then, it quickly gained traction, mostly with silly or low-effort videos — the kind of random content that could unexpectedly go viral. Those early days felt like the Wild West of the internet. There were no formulas, no thumbnails optimized for click-through rate, no SEO strategies — just creators experimenting and putting things out there.

On the creator side, it was pioneering. People weren’t thinking about careers or monetization — they were just having fun. But that raw experimentation laid the groundwork for what the platform has become.

Over the years, the platform — and its people — matured. YouTube evolved from a place of casual entertainment to a structured ecosystem where creators understand audiences, build businesses, and develop communities. Viewers moved from passively watching funny clips to actively subscribing, commenting, and curating their experience.

Now, with features like the homepage recommendation engine, custom playlists, membership programs, and even shorts, YouTube is no longer just a video site — it’s a complete content discovery and consumption platform.

From a user’s point of view, it went from ‘What’s funny today?’ to ‘What can I learn, be inspired by, or deeply engage with?’

YouTube’s impact is massive — not just culturally, but in how we consume, create, and interact with content today.

### Experiment 1: Home page recommended videos user customization
#### Objective
Test wether allowing the users to change the number of videos/size of the recommended videos displayed in the home page affect user engagement positively
#### Null hypothesis
Allowing users to customize the number and size of recommended videos on the home page does not result in a statistically significant change in user engagement metrics compared to the control group.
#### Alternative hypothesis
Allowing users to customize the number and size of recommended videos on the home page does result in a statistically significant improvement in user engagement metrics compared to the control group.

#### Leading metrics impacted
Time spent on home screen:
Likely decrease — users may spend less time scrolling and more time watching.

Click-through rate (CTR) on recommended videos:
Expected increase — personalized layout could lead to better visibility and relevance.

Engagement with customization settings:
Measures interest in feature itself.

Bounce rate from home page:
Could decrease if users find relevant videos faster.

#### Lagging metrics impacted
Total watch time per session:
Expected increase — users get to content faster.

Session length:
Could increase if users are more engaged.

Return rate / daily active users (DAU):
May increase if users feel more in control of their experience.

User satisfaction (via survey or thumbs-up rate on recommendations):
May increase with perceived control.

#### Test cell allocation

Control Group:
Standard home page with fixed layout (no customization options).

Treatment Group A:
Users can select between 3 layout densities (e.g., compact, standard, spacious).

Treatment Group B (optional):
Users can manually set number of recommended videos shown at once (e.g., 10–50).

Allocation Ratio:
50% Control / 25% Treatment A / 25% Treatment B (adjustable based on traffic volume).

### Experiment 2: Ability to generete content type views
#### Objective
Allow users to create custom content views (e.g., topic-specific home pages like "FPV Drones") instead of relying solely on algorithmic recommendations based on past activity.
This is similar to Reddit’s concept of channels/subreddits, giving users more topic control and discovery.

#### Null hypothesis
Allowing users to generate content-type views does not lead to a statistically significant change in user engagement, discovery, or satisfaction compared to the default recommendation system.

#### Alternative hypothesis\
Allowing users to generate content-type views leads to a statistically significant improvement in user engagement, discovery, or satisfaction compared to the default recommendation system.

#### Leading metrics impacted
Metrics likely to change early in user interaction:

  Topic view creation rate (feature engagement)
  Click-through rate (CTR) within custom views
  Time spent in custom views
  Session depth (e.g., videos watched per topic session)
  Bounce rate from home page (expected decrease)

#### Lagging metrics impacted
Total watch time per user

  Diversity of content consumed (breadth across topics)
  User retention / return rate over 7+ days
  Subscription rate to channels within topic views
  User satisfaction (survey feedback or like/dislike behavior)

#### Test cell allocation
Control Group:
Standard YouTube homepage with algorithmic recommendations only.

Treatment Group A:
Users can create custom content views manually (e.g., “FPV Drones”) via a “+ Add Topic” button.

Allocation Ratio:
50% Control / 50% Treatment A 
(Can be adjusted based on traffic and rollout stage.)


### Experiment 3: Resize to any width the video window on web
#### Objective
Allow users to resize the video player horizontally to suit their preferences — based on screen size, multitasking needs, or focus.
Example use cases:
  Shrinking video while reading or engaging with comments.
  Expanding video for a more immersive experience without going full screen.
  Adapting layout to wide monitors or split-screen multitasking.

#### Null hypothesis
Giving users the ability to resize the video window does not significantly impact user engagement, satisfaction, or overall video consumption behavior.

#### Alternative hypothesis
Allowing users to resize the video window positively impacts user engagement, satisfaction, or content interaction by supporting more personalized viewing experiences.

#### Leading metrics impacted
Resize interaction rate (feature engagement)

Time spent on video page
(may increase due to multitasking support)

Scroll activity during playback
(if users are reading comments while resizing)

Click-through rate on recommended videos (possible increase if layout allows easier browsing)


#### Lagging metrics impacted
Total watch time per session (could increase due to multitasking support)
Return visits / session frequency
User satisfaction scores (surveys, thumbs-up, NPS)
Drop-off rate in comment/engagement sections (may decrease if resizing helps)

#### Test cell allocation
Control Group:
Standard fixed-width video player.

Treatment Group A:
Users can resize video width freely via drag handles (responsive resizing behavior).

Allocation Ratio:
50% Control / 50% Treatment A 
(Can be adjusted based on traffic and rollout stage.)