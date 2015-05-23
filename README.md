
# Instructions

Build it:

    ./build.sh

Edit the `table_refresh.sh` script to your liking.

At the end of the run, you can try this Drill query (or something like it):


    use maprfs.vgonzalez;
    select convert_from(row_key, 'UTF8') as row_key, convert_from(convert_from(t.cf1.data, 'UTF8'), 'JSON') as data from `ticks` t limit 1;

You should get back something intelligible.