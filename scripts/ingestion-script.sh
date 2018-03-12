mkdir -p ~/player-data
cd ~/player-data
mkdir -p PlayerAttributeDataToBeProcessed PlayerPersonalDataToBeProcessed PlayerPlayingPositionDataToBeProcessed
mkdir -p layerAttributeDataArchive PlayerPersonalDataArchive PlayerPlayingPositionDataArchive
datasets=('https://s3-eu-west-1.amazonaws.com/aws-proserve-big-data/graduate-assignments/data/PlayerAttributeData.csv'	'https://s3-eu-west-1.amazonaws.com/aws-proserve-big-data/graduate-assignments/data/PlayerPersonalData.csv'	'https://s3-eu-west-1.amazonaws.com/aws-proserve-big-data/graduate-assignments/data/PlayerPlayingPositionData.csv')
for url in "${datasets[@]}"
do
	filename=$(basename "$url")
	fname="${filename%.*}"
	timestamp=$(date +%Y%m%d)
	cd ~/player-data
	cd "$fname""Archive"
	wget -c "$url" -O "$fname"_"$timestamp"".csv"
	cp "$fname"_"$timestamp"".csv" "~/player-data/""$fname""ToBeProcessed/"
done

aws	s3	sync	~/player-data/	s3://players-stats-s3