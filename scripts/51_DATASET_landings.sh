#!/bin/sh

R_LIBS_SITE="/usr/local/lib/R/site/%p-library/%v"
export R_LIBS_SITE
R_LIBS_USER="~/R/%p-library/%v"
export R_LIBS_USER

. /usr/local/etc/oracle/sjor.sh

exec R CMD BATCH --vanilla --slave /home/haf/einarhj/stasi/fishydata/scripts/51_DATASET_landings.R /home/haf/einarhj/stasi/fishydata/scripts/log/51_DATASET_landings.log
