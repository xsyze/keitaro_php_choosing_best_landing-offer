<?php

namespace Filters;

use Core\Filter\AbstractFilter;
use Core\Locale\LocaleService;
use Traffic\Model\StreamFilter;
use Traffic\RawClick;
use Traffic\Redis\Service\RedisStorageService;

class ChoosingBestOffer extends AbstractFilter
{
    const DEFAULT_TIMEZONE = 'Europe/Moscow';
    const KEITARO_HOST = ''; // HOST сервера КТ
    const KEITARO_API_KEY = ''; // API ключ
    const KEITARO_VERSION_API = 'v1';
    const CACHE_TIME = 0; //1800; // 30 минут
    const CACHE_KEY = "choosing-best"; // Ключ редис кеша
    const CAMPAIGN_ID_SUB = 2; // Subid, где записан adset.
    const ADSET_ID_SUB = 3; // Subid, где записан adset.
    const AD_ID_SUB = 4; // Subid, где записан adset.

    const DEFAULT_METRIC = "cr"; // Метрика по умолчанию
    const DEFAULT_DAYS = 2; // Количество дней по умолчанию
    const DEFAULT_WORK_MODE = 'landings'; // Режим работы по умолчанию
    const DEFAULT_LEADER_PERCENT = 80; // Процент лучшему по умолчанию
    const DEFAULT_STATISTICS = "campaign"; // Охват статистики по умолчанию
    const DEFAULT_MIN_CLICKS = 50; // Минимальное количество кликов для участия в выборе лучшего
    const DEFAULT_MIN_CONVERSIONS = 5; // Минимальное количество конверсий для участия в выборе 
    const DEBUG_ENABLED = true; // Лог работы
    const DEBUG_FILE =  '/var/www/keitaro/application/filters/ChoosingBestOffer.debug.log'; // Файл лога. Дать права 777

    private array $settings = [];
    private $redis = null;
 
    /**
     * @return array
     */
    public function getModes()
    {
        return [
            StreamFilter::ACCEPT => LocaleService::t('filters.binary_options.' . StreamFilter::ACCEPT),
            StreamFilter::REJECT => LocaleService::t('filters.binary_options.' . StreamFilter::REJECT),
        ];
    }

    /**
     * Опциональный фильтр по сущностям рекламной площадки
     * (если указаны SUB_ID`s)
     * 
     * @return string
     */
    private function getIdsOptions(): string
    {
        $options = "";

        if (self::CAMPAIGN_ID_SUB) {
            $options .= '<option value="ex_campaign_id">Кампания клика</option>';
        }
        if (self::ADSET_ID_SUB) {
            $options .= '<option value="ex_adset_id">Адсет клика</option>';
        }
        if (self::AD_ID_SUB) {
            $options .= '<option value="ex_ad_id">Объявление клика</option>';
        }

        return $options;
    }

    /**
     * UI шаблон фильтра
     * 
     * @return string
     */
    public function getTemplate()
    {
        $extIdsOption = $this->getIdsOptions();
        $defaultWorkMode = self::DEFAULT_WORK_MODE;
        $defaultMetric = self::DEFAULT_METRIC;
        $defaultDays = self::DEFAULT_DAYS;
        $defaultStatistics = self::DEFAULT_STATISTICS;
        $defaultLeaderPercent = self::DEFAULT_LEADER_PERCENT;
        $defaultCashTtl = self::CACHE_TIME;
        $defaultMinClicks = self::DEFAULT_MIN_CLICKS;
        $defaultMinConversions = self::DEFAULT_MIN_CONVERSIONS;

        return <<<HTML
            <div style="font-family: Arial, sans-serif; line-height: 1.5; max-width: 800px; margin: 20px auto; padding: 20px; border: 1px solid #ddd; border-radius: 8px; background-color: #f9f9f9;">
                <div style="display: flex; flex-wrap: wrap; gap: 20px;">
                    <div style="margin-bottom: 20px; text-align: right;">
                        <a 
                            href="https://github.com/xsyze/keitaro_php_choosing_best_landing-offer/tree/main" 
                            target="_blank" 
                            style="display: inline-block; padding: 8px 14px; background-color: #007bff; color: #ffffff; text-decoration: none; border-radius: 4px; font-size: 14px;"
                        >
                            📘 Открыть инструкцию
                        </a>
                    </div>
                    <div style="flex: 1 1 48%; min-width: 300px;">
                        <div style="margin-bottom: 15px">
                            <label style="display:block;font-weight:bold;margin-bottom:5px;"> С чем работать: </label>
                            <select ng-model="filter.payload.work_mode" ng-init="filter.payload.work_mode || (filter.payload.work_mode='$defaultWorkMode')" style="width:100%;padding:8px;border:1px solid #ccc;border-radius:4px;height:42px;margin-bottom:15px;">
                                <option value="landings">Только лендинги</option>
                                <option value="offers">Только офферы</option>
                                <option value="both">Лендинги + офферы</option>
                            </select>
                        </div>
                        
                        <div style="margin-bottom: 15px;">
                            <label for="metric" style="display: block; font-weight: bold; margin-bottom: 5px;">Какую метрику анализировать:</label>
                            <select 
                                id="metric"
                                ng-model="filter.payload.metric"
                                ng-init="filter.payload.metric || (filter.payload.metric='$defaultMetric')"
                                style="width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px; height: 42px;"
                            >
                                <option value="approve">Аппрув</option>
                                <option value="cr">CR</option>
                                <option value="crs">CR продажи</option>
                                <option value="cpa">CPA</option>
                                <option value="epc_confirmed">EPC</option>
                                <option value="lp_ctr">LP CTR</option>
                                <option value="roi_confirmed">ROI (подтвержденный)</option>
                            </select>
                        </div>

                        <div style="margin-bottom: 15px;">
                            <label for="statistics" style="display: block; font-weight: bold; margin-bottom: 5px;">Охват статистики:</label>
                            <select 
                                id="statistics"
                                ng-model="filter.payload.statistics"
                                ng-init="filter.payload.statistics || (filter.payload.statistics='$defaultStatistics')"
                                style="width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px; height: 42px;"
                            >
                                <option value="all">Общая по команде</option>
                                <option value="campaign">Текущая кампания</option>
                                <option value="creative">Наименование объявления фб</option>
                                {$extIdsOption}
                            </select>
                        </div>

                        <div style="margin-bottom: 15px;">
                            <label for="min_conversions" style="display: block; font-weight: bold; margin-bottom: 5px;">Минимум лидов для участия в&nbsp;выборе лучшего:</label>
                            <input 
                                id="min_conversions"
                                ng-model="filter.payload.min_conversions"
                                ng-init="filter.payload.min_conversions || (filter.payload.min_conversions='$defaultMinConversions')"
                                style="width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px; height: 42px;"
                            />
                        </div>
                    </div>

                    <div style="flex: 1 1 48%; min-width: 300px;">
                        <div style="margin-bottom: 15px;">
                            <label for="days" style="display: block; font-weight: bold; margin-bottom: 5px;">Дни статистики:</label>
                            <input 
                                type="range" 
                                id="days"
                                ng-model="filter.payload.days"
                                ng-init="filter.payload.days || (filter.payload.days=$defaultDays)"
                                min="1" 
                                max="7" 
                                step="1"
                                style="width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px;"
                            />
                            <span>{{filter.payload.days}} дней</span>
                        </div>

                        
                        <div style="margin-bottom: 15px;">
                            <label for="cache_ttl" style="display: block; font-weight: bold; margin-bottom: 5px;">Раз в N минут делать перераспределение</label>
                            <input 
                                type="range" 
                                id="cache_ttl"
                                ng-model="filter.payload.cache_ttl"
                                ng-init="filter.payload.cache_ttl || (filter.payload.cache_ttl=$defaultCashTtl)"
                                min="10" 
                                max="1440" 
                                step="10"
                                style="width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px;"
                            />
                            <span>{{filter.payload.cache_ttl}} минут</span> 
                        </div>

                        <div style="margin-bottom: 15px;">
                            <label for="share_of_percent" style="display: block; font-weight: bold; margin-bottom: 5px;">Процент доли трафика лучшему варианту:</label>
                            <input 
                                type="range" 
                                id="share_of_percent"
                                ng-model="filter.payload.leader_percent"
                                ng-init="filter.payload.leader_percent || (filter.payload.leader_percent=$defaultLeaderPercent)"
                                min="40" 
                                max="100" 
                                step="1"
                                style="width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px;"
                            />
                            <span>{{filter.payload.leader_percent}}%</span> 
                        </div>

                        <div style="margin-bottom: 15px;">
                            <label for="min_clicks" style="display: block; font-weight: bold; margin-bottom: 5px;">Минимум кликов для участия в&nbsp;выборе лучшего:</label>
                            <input 
                                type="range" 
                                id="min_clicks"
                                ng-model="filter.payload.min_clicks"
                                ng-init="filter.payload.min_clicks || (filter.payload.min_clicks=$defaultMinClicks)"
                                min="10" 
                                max="1000" 
                                step="10"
                                style="width: 100%; padding: 8px; border: 1px solid #ccc; border-radius: 4px;"
                            />
                            <span>{{filter.payload.min_clicks}} кликов</span> 
                        </div>

                    </div>
                </div>
            </div>
        HTML;
    }

    /**
     * @param StreamFilter $filter
     * @param RawClick $rawClick
     * @return bool
     */
    public function isPass(StreamFilter $filter, RawClick $rawClick)
    {
        try {
            if (self::DEBUG_ENABLED) {
                file_put_contents(
                    self::DEBUG_FILE,
                    PHP_EOL . 'RUN ' . ' | ' .  time() . ' | ' . $rawClick->getCampaignId() . ' | ' . $rawClick->getSubId() .  PHP_EOL, 
                    FILE_APPEND
                );
            }

            $this->redis = RedisStorageService::instance()->getOriginalClient();
            $streamId = $filter->getStreamId();
            $this->setSettings($filter);

            $this->debug('isPass start', [
                'stream_id' => $streamId,
                'initial_settings' => $this->settings
            ]);

            if ($this->settings['mode'] == StreamFilter::REJECT) {
                return true;
            }

            // Обработка лендингов
            if (in_array(
                $this->settings['workMode'], 
                ['landings', 'both'])
            ) {
                $this->processEntity(
                    $streamId,
                    'landing',
                    $rawClick,
                );
            }

            // Обработка офферов
            if (in_array(
                $this->settings['workMode'], 
                ['offers', 'both'])
            ) {
                $this->processEntity(
                    $streamId,
                    'offer',
                    $rawClick,
                );
            }

            return ($filter->getMode() == StreamFilter::ACCEPT);
        } catch (\Throwable $e) {
            $this->debug(
                'Error',
                [
                    'message' => $e->getMessage(),
                    'line' => $e->getLine(),
                ]
            );
        }
    }

    /**
     * @param int $streamId
     * @param string $entityType
     * @param RawClick $rawClick
     * @return void
     */
    private function processEntity(
        int $streamId,
        string $entityType,
        RawClick $rawClick
    ): void {
        $this->debug('Processing entity', [
            'stream_id' => $streamId,
            'entity' => $entityType
        ]);

        $items = $this->fetchStreamItems(
            $streamId, 
            $entityType
        );

        $this->debug('Fetched items', [
            'entity' => $entityType,
            'count' => count($items),
        ]);

        if (empty($items)) {
            return;
        }

        $selectedId = $this->determineBestEntity(
            $streamId,
            $entityType,
            $items,
            $rawClick,
        );

        $this->debug('Selected entity', [
            'entity' => $entityType,
            'selected_id' => $selectedId
        ]);

        if (!$selectedId) {
            return;
        }

        $this->updateStreamShares(
            $streamId,
            $entityType,
            $items,
            $selectedId
        );
    }

    /**
     * @param StreamFilter $filter
     * @return void
     */
    private function setSettings(
        StreamFilter $filter
    ): void {
        $payload = $filter->getPayload();

        $data = [
            'mode' => $filter->getMode(),
            'metric' => $payload['metric'] ?? self::DEFAULT_METRIC,
            'statistics' => $payload['statistics'] ?? self::DEFAULT_STATISTICS,
            'min_clicks' => max(
                10,
                min(
                    1000,
                    $payload['min_clicks'] ?? self::DEFAULT_MIN_CLICKS
                )
            ),
            'min_conversions' => max(
                0,
                min(
                    100,
                    $payload['min_conversions'] ?? self::DEFAULT_MIN_CONVERSIONS
                )
            ),
            'cache_ttl' => max(
                10,
                min (
                    1440, 
                    (int) $payload['cache_ttl'] ?? self::CACHE_TIME
                )
            ),
            'days' => max(
                1, 
                min(
                    7, 
                    (int) ($payload['days'] ?? self::DEFAULT_DAYS)
                )
            ),
            'leaderPercent' => max(
                1, 
                min(
                    100, 
                    (int) ($payload['leader_percent'] ?? self::DEFAULT_LEADER_PERCENT)
                )
            ),
            'workMode' => $payload['work_mode'] ?? self::DEFAULT_WORK_MODE,
        ];

        $this->settings = $data;
    }

    /**
     * Получение элементов распределения
     * 
     * @param int $streamId
     * @param string $entityType
     * @return array{id: mixed, state: mixed[]}
     */
    private function fetchStreamItems(int $streamId, string $entityType): array
    {
        $stream = json_decode(
            $this->apiRequest("/streams/$streamId"),
            true
        );

        $key = $entityType === 'offer' ? 'offers' : 'landings';
        $idField = $entityType === 'offer' ? 'offer_id' : 'landing_id';

        $items = $stream[$key] ?? [];

        $this->debug('fetchStreamItems', [
            'count' => count($items)
        ]);

        return array_map(function ($item) use ($idField) {
            return [
                'id' => $item[$idField],
                'state' => $item['state']
            ];
        }, $items);
    }

    /**
     * Добавление фильтра по параметру
     * 
     * @param string $statistics
     * @param RawClick $rawClick
     * @return array|array{expression: mixed, name: string, operator: string}
     */
    private function buildStatisticsFilter(
        RawClick $rawClick
    ) {
        switch ($this->settings['statistics']) {
            case 'campaign':
                return [
                    'name' => 'campaign_id',
                    'operator' => 'EQUALS',
                    'expression' => $rawClick->getCampaignId()
                ];

            case 'creative':
                return $rawClick->getCreativeId() ? [
                    'name' => 'creative_id',
                    'operator' => 'EQUALS',
                    'expression' => $rawClick->getCreativeId()
                ] : [];

            case 'ex_campaign_id':
                return $rawClick->getSubIdN(self::CAMPAIGN_ID_SUB) ? [
                    'name' => 'sub_id_' . self::CAMPAIGN_ID_SUB,
                    'operator' => 'EQUALS',
                    'expression' => $rawClick->getSubIdN(self::CAMPAIGN_ID_SUB)
                ] : [];

            case 'ex_adset_id':
                return $rawClick->getSubIdN(self::ADSET_ID_SUB) ? [
                    'name' => 'sub_id_' . self::ADSET_ID_SUB,
                    'operator' => 'EQUALS',
                    'expression' => $rawClick->getSubIdN(self::ADSET_ID_SUB)
                ] : [];

            case 'ex_ad_id':
                return $rawClick->getSubIdN(self::AD_ID_SUB) ? [
                    'name' => 'sub_id_' . self::AD_ID_SUB,
                    'operator' => 'EQUALS',
                    'expression' => $rawClick->getSubIdN(self::AD_ID_SUB)
                ] : [];

            default:
                return [];
        }
    }

    /**
     * Summary of generateCacheKey
     * @param int $streamId
     * @param string $entityType
     * @param RawClick $rawClick
     * @return string
     */
    private function generateCacheKey(
        int $streamId,
        string $entityType,
        RawClick $rawClick
    ): string {
        $cacheParts = [
            self::CACHE_KEY,
            $streamId,
            $entityType,
            $this->settings['days'],
            $this->settings['metric'],
            $this->settings['min_clicks'],
            $this->settings['min_conversions'],
            $this->settings['statistics'],
        ];

        if ($this->settings['statistics'] === 'campaign') {
            $cacheParts[] = $rawClick->getCampaignId();
        }

        if ($this->settings['statistics'] === 'creative') {
            $cacheParts[] = $rawClick->getCreativeId();
        }

        if ($this->settings['statistics'] === 'ex_campaign_id') {
            $cacheParts[] = $rawClick->getSubIdN(self::CAMPAIGN_ID_SUB);
        }

        if ($this->settings['statistics'] === 'ex_adset_id') {
            $cacheParts[] = $rawClick->getSubIdN(self::ADSET_ID_SUB);
        }

        if ($this->settings['statistics'] === 'ex_ad_id') {
            $cacheParts[] = $rawClick->getSubIdN(self::AD_ID_SUB);
        }

        return implode(':', $cacheParts);
    }

    /**
     * Выбираем лучший элемент
     * 
     * @param int $streamId
     * @param string $entityType
     * @param array $items
     * @param RawClick $rawClick
     */
    private function determineBestEntity(
        int $streamId,
        string $entityType,
        array $items,
        RawClick $rawClick
    ): ?int {
        $cacheKey = $this->generateCacheKey(
            $streamId,
            $entityType,
            $rawClick
        );

        $cached = $this->redis->get($cacheKey);

        if ($cached) {
            $this->debug('Select from cache', [
                'key' => $cacheKey,
                'cached' => $cached,
            ]);

            return (int) $cached;
        }

        $this->debug('Cache is missing');

        $activeItems = array_filter($items, fn($i) => $i['state'] === 'active');
        $ids = array_column($activeItems, 'id');
        $field = $entityType === 'offer' ? 'offer_id' : 'landing_id';

        $dateFrom = date(
            "Y-m-d", 
            strtotime("-" . ($this->settings['days'] - 1) . " day")
        );
        $dateTo = date('Y-m-d');

        $filters[] = ['name' => $field, 'operator' => 'IN_LIST', 'expression' => $ids];
        $statFilter = $this->buildStatisticsFilter(
            $rawClick
        );
        if (!empty($statFilter)) {
            $filters[] = $statFilter;
        }

        $params = [
            'columns' => [],
            'metrics' => ['conversions', 'clicks', $this->settings['metric']],
            'filters' => $filters,
            'grouping' => [$field],
            'range' => [
                'timezone' => self::DEFAULT_TIMEZONE,
                'from' => $dateFrom,
                'to' => $dateTo,
            ]
        ];

        $report = json_decode(
            $this->apiRequest('/report/build', 'POST', $params),
            true
        );

        $this->debug('Report response', [
            'entity' => $entityType,
            'params' => $params,
            'rows_count' => count($report['rows'] ?? []),
            'report' => $report
        ]);

        $bestId = null;
        $validRows = [];

        foreach ($report['rows'] ?? [] as $row) {
            if (
                $row['clicks'] < $this->settings['min_clicks'] ||
                $row['conversions'] < $this->settings['min_conversions']
            ) {
                continue;
            }

            if (!isset($row[$this->settings['metric']])) {
                continue;
            }

            $validRows[$row[$field]] = (float) $row[$this->settings['metric']];
        }

        // Если нет сущностей, прошедших порог
        if (empty($validRows)) {
            $this->debug('Not enough data to determine leader', [
                'entity' => $entityType
            ]);
            return null;
        }

        $maxValue = max($validRows);

        $leaders = array_keys(
            array_filter(
                $validRows,
                fn($v) => $v === $maxValue
            )
        );

        // Если лидеров несколько — берем случайного из них
        $bestId = $leaders[array_rand($leaders)];

        $this->debug('Leader selected', [
            'entity' => $entityType,
            'leader_id' => $bestId,
            'metric_name' => $this->settings['metric'],
            'metric' => $maxValue,
            'leaders_count' => count($leaders)
        ]);

        $this->redis->setex(
            $cacheKey,
            $this->settings['cache_ttl'] * 60,
            $bestId
        );

        return $bestId;
    }

    /**
     * Обновление процентов распределения сущностей
     * 
     * @param int $streamId
     * @param string $entityType
     * @param array $items
     * @param int $selectedId
     * @return void
     */
    private function updateStreamShares(
        int $streamId,
        string $entityType,
        array $items,
        int $selectedId
    ): void {

        $this->debug('Updating stream shares', [
            'stream_id' => $streamId,
            'entity' => $entityType,
            'leader_id' => $selectedId,
            'leader_percent' => $this->settings['leaderPercent']
        ]);

        $key = $entityType === 'offer' ? 'offers' : 'landings';
        $idField = $entityType === 'offer' ? 'offer_id' : 'landing_id';

        // 1️⃣ Разделяем активные и неактивные
        $activeItems = array_filter($items, function ($item) {
            return isset($item['state']) && $item['state'] === 'active';
        });

        $inactiveItems = array_filter($items, function ($item) {
            return !isset($item['state']) || $item['state'] !== 'active';
        });

        $activeItems = array_values($activeItems);
        $inactiveItems = array_values($inactiveItems);

        $totalActive = count($activeItems);

        $objects = [];

        // 2️⃣ Распределяем только среди active
        if ($totalActive > 0) {

            if ($totalActive === 1) {

                foreach ($activeItems as $item) {
                    $objects[] = (object)[
                        $idField => $item['id'],
                        'share' => 100,
                        'state' => $item['state'],
                    ];
                }
            } else {

                $remaining = 100 - $this->settings['leaderPercent'];
                $otherCount = $totalActive - 1;

                $sharePerOther = intdiv($remaining, $otherCount);

                $index = 0;

                foreach ($activeItems as $item) {

                    if ($item['id'] === $selectedId) {
                        $share = $this->settings['leaderPercent'];
                    } else {

                        // если это последний не-лидер
                        if ($index === $otherCount - 1) {
                            $share = $remaining - ($sharePerOther * ($otherCount - 1));
                        } else {
                            $share = $sharePerOther;
                        }

                        $index++;
                    }

                    $objects[] = (object)[
                        $idField => $item['id'],
                        'share' => $share,
                        'state' => $item['state'],
                    ];
                }
            }
        }

        // 3️⃣ Добавляем inactive без изменения share
        foreach ($inactiveItems as $item) {
            $objects[] = (object)[
                $idField => $item['id'],
                'share' => 0, // или можешь оставить прежний share если нужно
                'state' => $item['state'],
            ];
        }

        $this->apiRequest(
            "/streams/$streamId",
            'PUT',
            [$key => $objects]
        );
    }

    /**
     * @param string $endpoint
     * @param string $method
     * @param array $data
     * @return bool|string
     */
    private function apiRequest(string $endpoint, string $method = 'GET', array $data = []): ?string
    {
        $ch = curl_init();

        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_URL => self::KEITARO_HOST . "/admin_api/" . self::KEITARO_VERSION_API . $endpoint,
            CURLOPT_HTTPHEADER => [
                'Api-Key: ' . self::KEITARO_API_KEY,
                'Content-Type: application/json'
            ],
        ]);

        if ($method !== 'GET') {
            curl_setopt($ch, CURLOPT_CUSTOMREQUEST, $method);
            curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($data));
        }

        $response = curl_exec($ch);
        $error = curl_error($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);

        curl_close($ch);

        if ($error || $httpCode >= 400) {
            $this->debug('API ERROR', [
                'endpoint' => $endpoint,
                'method' => $method,
                'http_code' => $httpCode,
                'curl_error' => $error,
                'response' => $response
            ]);
        }

        return $response;
    }

    /**
     * @param string $message
     * @param array $context
     * @return void
     */
    private function debug(string $message, array $context = []): void
    {
        if (!self::DEBUG_ENABLED) {
            return;
        }

        $time = date('Y-m-d H:i:s');

        $log = "[$time] $message";

        if (!empty($context)) {
            $log .= ' | ' . json_encode($context, JSON_UNESCAPED_UNICODE);
        }

        $log .= PHP_EOL;

        file_put_contents(self::DEBUG_FILE, $log, FILE_APPEND);
    }
}
