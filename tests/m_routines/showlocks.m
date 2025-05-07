;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;								;
; Copyright (c) 2025 YottaDB LLC and/or its subsidiaries.	;
; All rights reserved.						;
;								;
;	This source code contains the intellectual property	;
;	of its copyright holder(s), and is made available	;
;	under a license.  If you do not know the terms of	;
;	the license, please stop and do not read further.	;
;								;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;
; Call ZSHOW "L" and return the result
entry ;
	new numlocks
	ZSHOW "L":numlocks
    
    if $data(numlocks("L",1))  do
    . ; There are held locks, and LOCK LEVEL > 0
	. set ret=numlocks("L",1)
    else  do
    . ; No locks are held, and LOCK LEVEL == 0
	. set ret="LOCK LEVEL=0"

    quit ret
