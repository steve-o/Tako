#!/bin/sh

# RSSL
exec ./tako.sh \
	"--session=rssl://nylabads2/?server-list=nylabads1,nylabads2"

# SSL
#./tako.sh \
#	"--session=ssled://nylabads2/?server-list=nylabads1,nylabads2"

