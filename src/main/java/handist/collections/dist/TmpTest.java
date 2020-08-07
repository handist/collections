/*******************************************************************************
 * Copyright (c) 2020 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under 
 * the terms of the Eclipse Public License 1.0 which accompanies this 
 * distribution, and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 *******************************************************************************/
package handist.collections.dist;

import java.util.function.Function;

import handist.collections.Chunk;
import handist.collections.LongRange;

public class TmpTest {
    static {
        TeamedPlaceGroup.setup();
    }

    public static void main(String[] args) {
        
        TeamedPlaceGroup pg = TeamedPlaceGroup.getWorld();
        DistCol<String> dcol = new DistCol<>(pg);
        Function<Long,String> gen = (Long index)-> {
            return "xx"+ index.toString();
        };
        dcol.add(new Chunk<String>(new LongRange(10, 100), gen));
        pg.broadcastFlat(()->{
            long offset = pg.myRank()* 1000L;
            dcol.add(new Chunk<String>(new LongRange(offset+10, offset+100), gen));
        });
        pg.broadcastFlat(()->{
           dcol.forEach((long i, String str)->{
              System.out.println("index:"+i+", msg:"+str); 
           });
        });        
    }

}
