/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.alibaba.himarket.repository;

import com.alibaba.himarket.entity.ProductLike;
import com.alibaba.himarket.support.enums.LikeStatus;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface ProductLikeRepository extends JpaRepository<ProductLike, Long> {

    /**
     * 根据产品ID和开发者ID查找点赞记录
     */
    Optional<ProductLike> findByProductIdAndDeveloperId(String productId, String developerId);

    /**
     * 根据产品ID和状态查找点赞记录
     */
    List<ProductLike> findByProductIdAndStatus(String productId, LikeStatus status);

    void deleteAllByProductId(String productId);

    /**
     * 根据产品ID统计点赞数
     */
    @Query(
            "SELECT COUNT(pl) FROM ProductLike pl WHERE pl.productId = :productId AND pl.status ="
                    + " com.alibaba.himarket.support.enums.LikeStatus.LIKED")
    Long countByProductIdAndStatus(@Param("productId") String productId);

    /**
     * 按产品ID分组统计点赞数
     */
    @Query(
            "SELECT pl.productId, COUNT(pl) FROM ProductLike pl WHERE pl.status ="
                    + " com.alibaba.himarket.support.enums.LikeStatus.LIKED GROUP BY pl.productId")
    List<Object[]> countLikesGroupedByProductId();
}
