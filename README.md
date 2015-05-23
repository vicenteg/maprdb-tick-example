
# Instructions

## Get it

    git clone https://github.com/vicenteg/maprdb-tick-example.git

## Build it

    ./build.sh

## Configure it

Edit the `table_refresh.sh` script to your liking.

## Generate some data

Assuming log-synth is installed to /opt, this generates 10 million rows of phony tick.

    /opt/log-synth/synth \
        -schema eoddata.json \
        -count $((10 * 10**6)) \
        -format json \
        -threads 50 \
        -output /mapr/se1/user/vgonzalez/2015-05-18


## Run it

    ./table_refresh.sh

At the end of the run, you can try this Drill query (or something like it):

    use maprfs.vgonzalez;
    select convert_from(row_key, 'UTF8') as row_key, convert_from(convert_from(t.cf1.data, 'UTF8'), 'JSON') as data from `ticks` t limit 1;

You should get back something intelligible.