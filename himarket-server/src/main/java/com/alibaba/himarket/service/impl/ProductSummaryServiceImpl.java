package com.alibaba.himarket.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.himarket.core.event.ProductDeletingEvent;
import com.alibaba.himarket.core.event.ProductSummaryUpdateEvent;
import com.alibaba.himarket.core.event.ProductUpdateEvent;
import com.alibaba.himarket.core.security.ContextHolder;
import com.alibaba.himarket.dto.params.product.QueryProductParam;
import com.alibaba.himarket.dto.result.common.PageResult;
import com.alibaba.himarket.dto.result.product.ProductSummaryResult;
import com.alibaba.himarket.entity.Product;
import com.alibaba.himarket.entity.ProductCategoryRelation;
import com.alibaba.himarket.entity.ProductPublication;
import com.alibaba.himarket.entity.ProductSummary;
import com.alibaba.himarket.repository.ChatRepository;
import com.alibaba.himarket.repository.ProductLikeRepository;
import com.alibaba.himarket.repository.ProductRepository;
import com.alibaba.himarket.repository.ProductSummaryRepository;
import com.alibaba.himarket.repository.SubscriptionRepository;
import com.alibaba.himarket.service.ProductCategoryService;
import com.alibaba.himarket.service.ProductSummaryService;
import jakarta.persistence.criteria.Predicate;
import jakarta.persistence.criteria.Root;
import jakarta.persistence.criteria.Subquery;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class ProductSummaryServiceImpl implements ProductSummaryService {
    private final ContextHolder contextHolder;

    private final ProductRepository productRepository;
    private final ProductSummaryRepository productSummaryRepository;
    private final ProductCategoryService productCategoryService;
    private final SubscriptionRepository subscriptionRepository;
    private final ChatRepository chatRepository;
    private final ProductLikeRepository productLikeRepository;

    @Override
    public void syncProductSummary(String productId) {
        try {
            // 获取产品信息
            Optional<Product> productOptional = productRepository.findByProductId(productId);
            if (!productOptional.isPresent()) {
                log.warn("Product not found with id: {}", productId);
                // 如果产品不存在，则删除对应的统计记录
                productSummaryRepository.deleteByProductId(productId);
                return;
            }

            Product product = productOptional.get();

            // 获取或创建统计记录
            ProductSummary summary =
                    productSummaryRepository
                            .findByProductId(productId)
                            .orElse(new ProductSummary());

            // 同步所有产品字段
            summary.setProductId(product.getProductId());
            summary.setName(product.getName());
            summary.setType(product.getType());
            summary.setDescription(product.getDescription());
            summary.setIcon(product.getIcon());

            // 获取并设置订阅数
            Long subscriptionCount =
                    subscriptionRepository.countApprovedSubscriptionsByProductId(productId);
            summary.setSubscriptionCount(subscriptionCount.intValue());

            // 获取并设置调用量
            Long usageCount = chatRepository.countChatsByProductId(productId);
            summary.setUsageCount(usageCount.intValue());

            // 获取并设置点赞数
            Long likesCount = productLikeRepository.countByProductIdAndStatus(productId);
            summary.setLikesCount(likesCount.intValue());

            // 保存记录
            productSummaryRepository.save(summary);
            log.info("Successfully synced product summary for product: {}", productId);
        } catch (Exception e) {
            log.error("Error syncing product summary for product: {}", productId, e);
        }
    }

    /**
     * 批量更新统计表，确保统计数据与产品表一致
     */
    @Override
    public void syncAllProductSummary() {
        List<Product> allProducts = productRepository.findAll();
        // 获取所有产品的统计数据
        Map<String, Integer> subscriptionCounts = getSubscriptionCounts();
        Map<String, Integer> usageCounts = getUsageCounts();
        Map<String, Integer> likesCounts = getLikesCounts();
        // 获取所有现有的统计记录
        List<ProductSummary> existingStats = productSummaryRepository.findAll();
        Map<String, ProductSummary> existingStatsMap =
                existingStats.stream()
                        .collect(Collectors.toMap(ProductSummary::getProductId, s -> s));

        // 准备要更新的记录列表
        List<ProductSummary> productSummaries = new ArrayList<>();

        // 为每个产品创建或更新ProductSummary
        for (Product product : allProducts) {
            String productId = product.getProductId();
            ProductSummary summary = existingStatsMap.getOrDefault(productId, new ProductSummary());

            // 同步所有产品字段
            summary.setProductId(product.getProductId());
            summary.setName(product.getName());
            summary.setType(product.getType());
            summary.setDescription(product.getDescription());
            summary.setIcon(product.getIcon());
            summary.setCreateAt(product.getCreateAt());
            summary.setUpdatedAt(product.getUpdatedAt());

            // 设置订阅数（如果没有订阅则默认为0）
            Integer subscriptionCount = subscriptionCounts.getOrDefault(productId, 0);
            summary.setSubscriptionCount(subscriptionCount);

            // 设置调用量（如果没有调用记录则默认为0）
            Integer usageCount = usageCounts.getOrDefault(productId, 0);
            summary.setUsageCount(usageCount);

            // 设置点赞数（如果没有点赞记录则默认为0）
            Integer likesCount = likesCounts.getOrDefault(productId, 0);
            summary.setLikesCount(likesCount);

            productSummaries.add(summary);
        }

        // 批量保存更新
        if (!productSummaries.isEmpty()) {
            // 分批保存以避免大事务
            saveProductSummaryBatch(productSummaries);
            log.info("更新了 {} 条产品统计记录", productSummaries.size());
        }

        // 删除不再存在的产品的统计记录
        List<String> allProductIds =
                allProducts.stream().map(Product::getProductId).collect(Collectors.toList());

        List<ProductSummary> statsToDelete =
                existingStats.stream()
                        .filter(summary -> !allProductIds.contains(summary.getProductId()))
                        .collect(Collectors.toList());

        if (!statsToDelete.isEmpty()) {
            // 分批删除以避免大事务
            deleteProductSummaryBatch(statsToDelete);
            log.info("删除了 {} 条过时的产品统计记录", statsToDelete.size());
        }
    }

    /**
     * 获取所有产品的订阅数
     *
     * @return 产品ID到订阅数的映射
     */
    private Map<String, Integer> getSubscriptionCounts() {
        List<Object[]> results =
                subscriptionRepository.countApprovedSubscriptionsGroupedByProductId();
        Map<String, Integer> subscriptionCounts = new HashMap<>();
        for (Object[] result : results) {
            String productId = (String) result[0];
            Long count = (Long) result[1];
            subscriptionCounts.put(productId, count.intValue());
        }
        return subscriptionCounts;
    }

    /**
     * 获取所有产品的调用量
     *
     * @return 产品ID到调用量的映射
     */
    private Map<String, Integer> getUsageCounts() {
        List<Object[]> results = chatRepository.countChatsGroupedByProductId();
        Map<String, Integer> usageCounts = new HashMap<>();
        for (Object[] result : results) {
            String productId = (String) result[0];
            Long count = (Long) result[1];
            usageCounts.put(productId, count.intValue());
        }
        return usageCounts;
    }

    /**
     * 获取所有产品的点赞数
     *
     * @return 产品ID到点赞数的映射
     */
    private Map<String, Integer> getLikesCounts() {
        List<Object[]> results = productLikeRepository.countLikesGroupedByProductId();
        Map<String, Integer> likesCounts = new HashMap<>();
        for (Object[] result : results) {
            String productId = (String) result[0];
            Long count = (Long) result[1];
            likesCounts.put(productId, count.intValue());
        }
        return likesCounts;
    }

    @Override
    public PageResult<ProductSummaryResult> listPortalProducts(
            QueryProductParam param, Pageable pageable) {
        if (contextHolder.isDeveloper()) {
            param.setPortalId(contextHolder.getPortal());
        }
        if (productSummaryRepository.count() != productRepository.count()) {
            syncAllProductSummary();
        }
        Page<ProductSummary> products =
                productSummaryRepository.findAll(buildSpecificationSummary(param), pageable);
        return new PageResult<ProductSummaryResult>()
                .convertFrom(
                        products,
                        product -> {
                            ProductSummaryResult result =
                                    new ProductSummaryResult().convertFrom(product);
                            fillProduct(result);
                            return result;
                        });
    }

    private void fillProduct(ProductSummaryResult product) {
        // Fill product category information
        product.setCategories(
                productCategoryService.listCategoriesForProduct(product.getProductId()));
    }

    private Specification<ProductSummary> buildSpecificationSummary(QueryProductParam param) {
        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (StrUtil.isNotBlank(param.getPortalId())) {
                Subquery<String> subquery = query.subquery(String.class);
                Root<ProductPublication> publicationRoot = subquery.from(ProductPublication.class);
                subquery.select(publicationRoot.get("productId"))
                        .where(cb.equal(publicationRoot.get("portalId"), param.getPortalId()));
                predicates.add(root.get("productId").in(subquery));
            }

            if (param.getType() != null) {
                predicates.add(cb.equal(root.get("type"), param.getType()));
            }

            if (param.getStatus() != null) {
                predicates.add(cb.equal(root.get("status"), param.getStatus()));
            }

            if (StrUtil.isNotBlank(param.getName())) {
                String likePattern = "%" + param.getName() + "%";
                predicates.add(cb.like(root.get("name"), likePattern));
            }

            if (CollUtil.isNotEmpty(param.getCategoryIds())) {
                Subquery<String> subquery = query.subquery(String.class);
                Root<ProductCategoryRelation> relationRoot =
                        subquery.from(ProductCategoryRelation.class);
                subquery.select(relationRoot.get("productId"))
                        .where(relationRoot.get("categoryId").in(param.getCategoryIds()));
                predicates.add(root.get("productId").in(subquery));
            }

            if (StrUtil.isNotBlank(param.getExcludeCategoryId())) {
                Subquery<String> subquery = query.subquery(String.class);
                Root<ProductCategoryRelation> relationRoot =
                        subquery.from(ProductCategoryRelation.class);
                subquery.select(relationRoot.get("productId"))
                        .where(
                                cb.equal(
                                        relationRoot.get("categoryId"),
                                        param.getExcludeCategoryId()));
                predicates.add(cb.not(root.get("productId").in(subquery)));
            }

            return cb.and(predicates.toArray(new Predicate[0]));
        };
    }

    /**
     * 批量删除产品统计数据
     *
     * @param statisticsList 统计数据列表
     */
    @Transactional
    public void deleteProductSummaryBatch(List<ProductSummary> statisticsList) {
        // 分批删除以避免大事务
        int batchSize = 100;
        for (int i = 0; i < statisticsList.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, statisticsList.size());
            List<ProductSummary> batch = statisticsList.subList(i, endIndex);
            productSummaryRepository.deleteAll(batch);
            log.debug("删除了 {} 条产品统计记录", batch.size());
        }
    }

    /**
     * 批量保存产品统计数据
     *
     * @param statisticsList 统计数据列表
     */
    @Transactional
    public void saveProductSummaryBatch(List<ProductSummary> statisticsList) {
        // 分批保存以避免大事务
        int batchSize = 100;
        for (int i = 0; i < statisticsList.size(); i += batchSize) {
            int endIndex = Math.min(i + batchSize, statisticsList.size());
            List<ProductSummary> batch = statisticsList.subList(i, endIndex);
            productSummaryRepository.saveAll(batch);
            log.debug("保存了 {} 条产品统计记录", batch.size());
        }
    }

    /**
     * 监听产品统计更新事件并异步处理
     *
     * @param event 产品统计更新事件
     */
    @EventListener
    @Async("taskExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleProductSummaryUpdate(ProductSummaryUpdateEvent event) {
        try {
            String productId = event.getProductId();
            // 改为全量统计，重新计算该产品的所有统计数据
            syncProductSummary(productId);

            log.info("Recomputed product statistics for product: {}", productId);
        } catch (Exception e) {
            log.error(
                    "Error recomputing product statistics for product: " + event.getProductId(), e);
        }
    }

    @EventListener
    @Async("taskExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleProductSummaryCreation(ProductUpdateEvent event) {
        String productId = event.getProductId();
        log.info("Handling product update for product {}", productId);
        this.syncProductSummary(productId);
        log.info("Synced product summary for product {}", productId);
    }

    @EventListener
    @Async("taskExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleProductSummaryDeletion(ProductDeletingEvent event) {
        String productId = event.getProductId();
        log.info("Handling product deletion for product {}", productId);
        productSummaryRepository.deleteByProductId(productId);
        log.info("Synced product deletion for product {}", productId);
    }
}
