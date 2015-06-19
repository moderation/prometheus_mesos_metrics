package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/log"
)

const concurrentFetch = 100

// Commandline flags .
var (
	addr           = flag.String("web.listen-address", ":9106", "Address to listen on for web interface and telemetry")
	autoDiscover   = flag.Bool("exporter.discovery", false, "Discover all Mesos slaves")
	localURL       = flag.String("exporter.local-url", "http://127.0.0.1:5051", "URL to the local Mesos slave")
	masterURL      = flag.String("exporter.discovery.master-url", "http://mesos-master.example.com:5050", "Mesos master URL")
	metricsPath    = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics")
	scrapeInterval = flag.Duration("exporter.interval", (60 * time.Second), "Scrape interval duration")
)

var (
	// variableLabels = []string{"task", "slave", "framework_id"}
	variableLabels = []string{"slave"}

	slaveCPUPercentDesc = prometheus.NewDesc(
		"slave_cpus_percent",
		"Slave CPU percent.",
		variableLabels, nil,
	)
	slaveCPUTotalDesc = prometheus.NewDesc(
		"slave_cpus_total",
		"Slave CPUs total.",
		variableLabels, nil,
	)
	slaveCpusUsedDesc = prometheus.NewDesc(
		"slave_cpus_used",
		"Slave CPUs used.",
		variableLabels, nil,
	)
	slaveDiskPercentDesc = prometheus.NewDesc(
		"slave_disk_percent",
		"Slave disk usage as a percentage.",
		variableLabels, nil,
	)
	slaveDiskTotalDesc = prometheus.NewDesc(
		"slave_disk_total",
		"Total of disk available on the slave.",
		variableLabels, nil,
	)
	slaveDiskUsedDesc = prometheus.NewDesc(
		"slave_disk_used",
		"Total of disk used on the slave.",
		variableLabels, nil,
	)
	slaveExecutorsRegisteringDesc = prometheus.NewDesc(
		"slave_executors_registering",
		"Number of slave executors registering.",
		variableLabels, nil,
	)
	slaveExecutorsRunningDesc = prometheus.NewDesc(
		"slave_executors_running",
		"Number of slave executors running.",
		variableLabels, nil,
	)
	slaveExecutorsTerminatedDesc = prometheus.NewDesc(
		"slave_executors_terminated",
		"Number of slave executors terminated.",
		variableLabels, nil,
	)
	slaveExecutorsTerminatingDesc = prometheus.NewDesc(
		"slave_executors_terminating",
		"Number of slave executors terminating.",
		variableLabels, nil,
	)
	slaveFrameworksActiveDesc = prometheus.NewDesc(
		"slave_frameworks_active",
		"Number of active slave frameworks.",
		variableLabels, nil,
	)
	slaveInvalidFrameworkMessagesDesc = prometheus.NewDesc(
		"slave_invalid_framework_messages",
		"Number of slave invalid framework messages.",
		variableLabels, nil,
	)
	slaveInvalidStatusUpdatesDesc = prometheus.NewDesc(
		"slave_invalid_status_updates",
		"Number of slave invalid status updates.",
		variableLabels, nil,
	)
	slaveMemPercentDesc = prometheus.NewDesc(
		"slave_mem_percent",
		"Slave memory percent.",
		variableLabels, nil,
	)
	slaveMemTotalDesc = prometheus.NewDesc(
		"slave_mem_total",
		"Slave memory total.",
		variableLabels, nil,
	)
	slaveMemUsedDesc = prometheus.NewDesc(
		"slave_mem_used",
		"Slave memory used.",
		variableLabels, nil,
	)
	slaveRecoveryErrorsDesc = prometheus.NewDesc(
		"slave_recovery_errors",
		"Slave recovery errors.",
		variableLabels, nil,
	)
	slaveRegisteredDesc = prometheus.NewDesc(
		"slave_registered",
		"Slave registered.",
		variableLabels, nil,
	)
	slaveTaskFailedDesc = prometheus.NewDesc(
		"slave_tasks_failed",
		"Slave tasks failed.",
		variableLabels, nil,
	)
	slaveTaskFinishedDesc = prometheus.NewDesc(
		"slave_tasks_finished",
		"Slave tasks finished.",
		variableLabels, nil,
	)
	slaveTaskKilledDesc = prometheus.NewDesc(
		"slave_tasks_killed",
		"Slave tasks killed.",
		variableLabels, nil,
	)
	slaveTaskLostDesc = prometheus.NewDesc(
		"slave_tasks_lost",
		"Slave tasks lost.",
		variableLabels, nil,
	)
	slaveTaskRunningDesc = prometheus.NewDesc(
		"slave_tasks_running",
		"Slave tasks running.",
		variableLabels, nil,
	)
	slaveTaskStagingDesc = prometheus.NewDesc(
		"slave_tasks_staging",
		"Slave tasks staging.",
		variableLabels, nil,
	)
	slaveTaskStartingDesc = prometheus.NewDesc(
		"slave_tasks_starting",
		"Slave tasks starting.",
		variableLabels, nil,
	)
	slaveUptimeSecsDesc = prometheus.NewDesc(
		"slave_uptime_secs",
		"Slave uptime seconds.",
		variableLabels, nil,
	)
	slaveValidFrameworkMessagesDesc = prometheus.NewDesc(
		"slave_valid_framework_messages",
		"Slave valid framework messages.",
		variableLabels, nil,
	)
	slaveValidStatusUpdatesDesc = prometheus.NewDesc(
		"slave_valid_status_updates",
		"Slave valid status updates.",
		variableLabels, nil,
	)
	slaveSystemCpusTotalDesc = prometheus.NewDesc(
		"slave_system_cpus_total",
		"Total slave CPUs.",
		variableLabels, nil,
	)
	slaveSystemLoad15minDesc = prometheus.NewDesc(
		"slave_system_load_15min",
		"Slave system load 15 minutes.",
		variableLabels, nil,
	)
	slaveSystemLoad1minDesc = prometheus.NewDesc(
		"slave_system_load_1min",
		"Slave system load 1 minute.",
		variableLabels, nil,
	)
	slaveSystemLoad5minDesc = prometheus.NewDesc(
		"slave_system_load_5min",
		"Slave system load 5 minutes.",
		variableLabels, nil,
	)
	slaveSystemMemFreeBytesDesc = prometheus.NewDesc(
		"slave_system_mem_free_bytes",
		"Free memory on the slave in bytes.",
		variableLabels, nil,
	)
	slaveSystemMemTotalBytesDesc = prometheus.NewDesc(
		"slave_system_mem_total_bytes",
		"Total memory on the slave in bytes.",
		variableLabels, nil,
	)
)

var httpClient = http.Client{
	Timeout: 5 * time.Second,
}

type exporterOpts struct {
	autoDiscover bool
	interval     time.Duration
	localURL     string
	masterURL    string
}

type periodicExporter struct {
	sync.RWMutex
	errors  *prometheus.CounterVec
	metrics []prometheus.Metric
	opts    *exporterOpts
	slaves  struct {
		sync.Mutex
		urls []string
	}
}

func newMesosExporter(opts *exporterOpts) *periodicExporter {
	e := &periodicExporter{
		errors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "prometheus_mesos_exporter",
				Name:      "slave_scrape_errors_total",
				Help:      "Current total scrape errors",
			},
			[]string{"slave"},
		),
		opts: opts,
	}
	e.slaves.urls = []string{e.opts.localURL}

	if e.opts.autoDiscover {
		log.Info("auto discovery enabled from command line flag.")

		// Update nr. of mesos slaves every 10 minutes
		e.updateSlaves()
		go runEvery(e.updateSlaves, 10*time.Minute)
	}

	// Fetch slave metrics every interval
	go runEvery(e.scrapeSlaves, e.opts.interval)

	return e
}

func (e *periodicExporter) Describe(ch chan<- *prometheus.Desc) {
	e.rLockMetrics(func() {
		for _, m := range e.metrics {
			ch <- m.Desc()
		}
	})
	e.errors.MetricVec.Describe(ch)
}

func (e *periodicExporter) Collect(ch chan<- prometheus.Metric) {
	e.rLockMetrics(func() {
		for _, m := range e.metrics {
			ch <- m
		}
	})
	e.errors.MetricVec.Collect(ch)
}

func (e *periodicExporter) fetch(urlChan <-chan string, metricsChan chan<- prometheus.Metric, wg *sync.WaitGroup) {
	defer wg.Done()

	for u := range urlChan {
		u, err := url.Parse(u)
		if err != nil {
			log.Warn("could not parse slave URL: ", err)
			continue
		}

		host, _, err := net.SplitHostPort(u.Host)
		if err != nil {
			log.Warn("could not parse network address: ", err)
			continue
		}

		monitorURL := fmt.Sprintf("%s/metrics/snapshot", u)
		resp, err := httpClient.Get(monitorURL)
		if err != nil {
			log.Warn(err)
			e.errors.WithLabelValues(host).Inc()
			continue
		}
		defer resp.Body.Close()

		var stats Metrics
		if err = json.NewDecoder(resp.Body).Decode(&stats); err != nil {
			log.Warn("failed to deserialize response: ", err)
			e.errors.WithLabelValues(host).Inc()
			continue
		}

		metricsChan <- prometheus.MustNewConstMetric(
			slaveCPUPercentDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveCpusPercent),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveCPUTotalDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveCpusTotal),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveCpusUsedDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveCpusUsed),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveDiskPercentDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveDiskPercent),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveDiskTotalDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveDiskTotal),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveDiskUsedDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveDiskUsed),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveExecutorsRegisteringDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveExecutorsRegistering),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveExecutorsRunningDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveExecutorsRunning),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveExecutorsTerminatedDesc,
			prometheus.CounterValue,
			float64(stats.SlaveExecutorsTerminated),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveExecutorsTerminatingDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveExecutorsTerminating),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveFrameworksActiveDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveFrameworksActive),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveInvalidFrameworkMessagesDesc,
			prometheus.CounterValue,
			float64(stats.SlaveInvalidFrameworkMessages),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveInvalidStatusUpdatesDesc,
			prometheus.CounterValue,
			float64(stats.SlaveInvalidStatusUpdates),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveMemPercentDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveMemPercent),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveMemTotalDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveMemTotal),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveMemUsedDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveMemUsed),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveRecoveryErrorsDesc,
			prometheus.CounterValue,
			float64(stats.SlaveRecoveryErrors),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveRegisteredDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveRegistered),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveTaskFailedDesc,
			prometheus.CounterValue,
			float64(stats.SlaveTasksFailed),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveTaskFinishedDesc,
			prometheus.CounterValue,
			float64(stats.SlaveTasksFinished),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveTaskKilledDesc,
			prometheus.CounterValue,
			float64(stats.SlaveTasksKilled),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveTaskLostDesc,
			prometheus.CounterValue,
			float64(stats.SlaveTasksLost),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveTaskRunningDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveTasksRunning),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveTaskStagingDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveTasksStaging),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveTaskStartingDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveTasksStarting),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveUptimeSecsDesc,
			prometheus.GaugeValue,
			float64(stats.SlaveUptimeSecs),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveValidFrameworkMessagesDesc,
			prometheus.CounterValue,
			float64(stats.SlaveValidFrameworkMessages),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveValidStatusUpdatesDesc,
			prometheus.CounterValue,
			float64(stats.SlaveValidStatusUpdates),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveSystemCpusTotalDesc,
			prometheus.GaugeValue,
			float64(stats.SystemCpusTotal),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveSystemLoad15minDesc,
			prometheus.GaugeValue,
			float64(stats.SystemLoad15min),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveSystemLoad1minDesc,
			prometheus.GaugeValue,
			float64(stats.SystemLoad1min),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveSystemLoad5minDesc,
			prometheus.GaugeValue,
			float64(stats.SystemLoad5min),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveSystemMemFreeBytesDesc,
			prometheus.GaugeValue,
			float64(stats.SystemMemFreeBytes),
			host,
		)
		metricsChan <- prometheus.MustNewConstMetric(
			slaveSystemMemTotalBytesDesc,
			prometheus.GaugeValue,
			float64(stats.SystemMemTotalBytes),
			host,
		)
	}
}

func (e *periodicExporter) rLockMetrics(f func()) {
	e.RLock()
	defer e.RUnlock()
	f()
}

func (e *periodicExporter) setMetrics(ch chan prometheus.Metric) {
	metrics := make([]prometheus.Metric, 0)
	for metric := range ch {
		metrics = append(metrics, metric)
	}

	e.Lock()
	e.metrics = metrics
	e.Unlock()
}

func (e *periodicExporter) scrapeSlaves() {
	e.slaves.Lock()
	urls := make([]string, len(e.slaves.urls))
	copy(urls, e.slaves.urls)
	e.slaves.Unlock()

	urlCount := len(urls)
	log.Debugf("active slaves: %d", urlCount)

	urlChan := make(chan string)
	metricsChan := make(chan prometheus.Metric)
	go e.setMetrics(metricsChan)

	poolSize := concurrentFetch
	if urlCount < concurrentFetch {
		poolSize = urlCount
	}

	log.Debugf("creating fetch pool of size %d", poolSize)

	var wg sync.WaitGroup
	wg.Add(poolSize)
	for i := 0; i < poolSize; i++ {
		go e.fetch(urlChan, metricsChan, &wg)
	}

	for _, url := range urls {
		urlChan <- url
	}
	close(urlChan)

	wg.Wait()
	close(metricsChan)
}

func (e *periodicExporter) updateSlaves() {
	log.Debug("discovering slaves...")

	// This will redirect us to the elected mesos master
	redirectURL := fmt.Sprintf("%s/master/redirect", e.opts.masterURL)
	rReq, err := http.NewRequest("GET", redirectURL, nil)
	if err != nil {
		panic(err)
	}

	tr := http.Transport{
		DisableKeepAlives: true,
	}
	rresp, err := tr.RoundTrip(rReq)
	if err != nil {
		log.Warn(err)
		return
	}
	defer rresp.Body.Close()

	// This will/should return http://master.ip:5050
	masterLoc := rresp.Header.Get("Location")
	if masterLoc == "" {
		log.Warnf("%d response missing Location header", rresp.StatusCode)
		return
	}

	log.Debugf("current elected master at: %s", masterLoc)

	// Find all active slaves
	stateURL := fmt.Sprintf("%s/master/state.json", masterLoc)
	resp, err := http.Get(stateURL)
	if err != nil {
		log.Warn(err)
		return
	}
	defer resp.Body.Close()

	type slave struct {
		Active   bool   `json:"active"`
		Hostname string `json:"hostname"`
		Pid      string `json:"pid"`
	}

	var req struct {
		Slaves []*slave `json:"slaves"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&req); err != nil {
		log.Warnf("failed to deserialize request: %s", err)
		return
	}

	var slaveURLs []string
	for _, slave := range req.Slaves {
		if slave.Active {
			// Extract slave port from pid
			_, port, err := net.SplitHostPort(slave.Pid)
			if err != nil {
				port = "5051"
			}
			url := fmt.Sprintf("http://%s:%s", slave.Hostname, port)

			slaveURLs = append(slaveURLs, url)
		}
	}

	log.Debugf("%d slaves discovered", len(slaveURLs))

	e.slaves.Lock()
	e.slaves.urls = slaveURLs
	e.slaves.Unlock()
}

func runEvery(f func(), interval time.Duration) {
	for _ = range time.NewTicker(interval).C {
		f()
	}
}

func main() {
	flag.Parse()

	opts := &exporterOpts{
		autoDiscover: *autoDiscover,
		interval:     *scrapeInterval,
		localURL:     strings.TrimRight(*localURL, "/"),
		masterURL:    strings.TrimRight(*masterURL, "/"),
	}
	exporter := newMesosExporter(opts)
	prometheus.MustRegister(exporter)

	http.Handle(*metricsPath, prometheus.Handler())
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "OK")
	})
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, *metricsPath, http.StatusMovedPermanently)
	})

	log.Info("starting prometheus_mesos_exporter on ", *addr)

	log.Fatal(http.ListenAndServe(*addr, nil))
}

type Metrics struct {
	SlaveCpusPercent              float32 `json:"slave/cpus_percent"`
	SlaveCpusTotal                uint16  `json:"slave/cpus_total"`
	SlaveCpusUsed                 float32 `json:"slave/cpus_used"`
	SlaveDiskPercent              float32 `json:"slave/disk_percent"`
	SlaveDiskTotal                uint32  `json:"slave/disk_total"`
	SlaveDiskUsed                 uint32  `json:"slave/disk_used"`
	SlaveExecutorsRegistering     uint16  `json:"slave/executors_registering"`
	SlaveExecutorsRunning         uint16  `json:"slave/executors_running"`
	SlaveExecutorsTerminated      uint16  `json:"slave/executors_terminated"`
	SlaveExecutorsTerminating     uint16  `json:"slave/executors_terminating"`
	SlaveFrameworksActive         uint16  `json:"slave/frameworks_active"`
	SlaveInvalidFrameworkMessages uint16  `json:"slave/invalid_framework_messages"`
	SlaveInvalidStatusUpdates     uint16  `json:"slave/invalid_status_updates"`
	SlaveMemPercent               float32 `json:"slave/mem_percent"`
	SlaveMemTotal                 uint32  `json:"slave/mem_total"`
	SlaveMemUsed                  uint32  `json:"slave/mem_used"`
	SlaveRecoveryErrors           uint16  `json:"slave/recovery_errors"`
	SlaveRegistered               uint16  `json:"slave/registered"`
	SlaveTasksFailed              uint16  `json:"slave/tasks_failed"`
	SlaveTasksFinished            uint16  `json:"slave/tasks_finished"`
	SlaveTasksKilled              uint16  `json:"slave/tasks_killed"`
	SlaveTasksLost                uint16  `json:"slave/tasks_lost"`
	SlaveTasksRunning             uint16  `json:"slave/tasks_running"`
	SlaveTasksStaging             uint16  `json:"slave/tasks_staging"`
	SlaveTasksStarting            uint16  `json:"slave/tasks_starting"`
	SlaveUptimeSecs               float32 `json:"slave/uptime_secs"`
	SlaveValidFrameworkMessages   uint16  `json:"slave/valid_framework_messages"`
	SlaveValidStatusUpdates       uint16  `json:"slave/valid_status_updates"`
	SystemCpusTotal               uint16  `json:"system/cpus_total"`
	SystemLoad15min               float32 `json:"system/load_15min"`
	SystemLoad1min                float32 `json:"system/load_1min"`
	SystemLoad5min                float32 `json:"system/load_5min"`
	SystemMemFreeBytes            uint64  `json:"system/mem_free_bytes"`
	SystemMemTotalBytes           uint64  `json:"system/mem_total_bytes"`
}
