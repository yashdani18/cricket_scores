# Sports Analytics (Cricket)

This is a personal project to collect and analyze cricket scores for tournaments around the world.
The primary objective of this project is to 
create data pipelines that enable collection and analysis 
of cricket scores for tournaments around the world. 
Currently, I have built pipelines for IPL2024 tournament 
but can be extended to other tournaments 
in the future.

## Motivation
The primary motivator for developing this project was the need to receive near real-time (NRT) updates about a cricket game. For example, if a team is batting at a current run rate of more than 10 runs/over after the powerplay, it is probably a match worth watching.

Since I am using web scraping to collect data, I have leveraged polling to receive periodic updates about the game. Based on my aforementioned criteria for alerts, I shoot myself a text using Telegram bots when the criteria is met.

## Data sources
* [Cricbuzz](https://www.cricbuzz.com)
##### Disclaimer: The data I scrape is used for personal consumption only and is not monetized in any way.


## Libraries
* beautifulsoup4
* pandas
* mysql-connector-python

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.