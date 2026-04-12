# Prefect Automations Module
# Manages common automation configurations with environment-specific overrides

# Crash Zombie Flows - Marks flow runs as crashed when they miss heartbeats
resource "prefect_automation" "crash_zombie_flows" {
  name        = "crash-zombie-flows"
  description = ""
  enabled     = var.crash_zombie_flows_enabled

  trigger = {
    event = {
      match         = jsonencode({ "prefect.resource.id" : "prefect.flow-run.*" })
      match_related = jsonencode({})
      after         = ["prefect.flow-run.heartbeat"]
      expect        = ["prefect.flow-run.*"]
      for_each      = ["prefect.resource.id"]
      posture       = "Proactive"
      threshold     = 1
      within        = 120
    }
  }

  actions = [
    {
      type    = "change-flow-run-state"
      state   = "CRASHED"
      message = "Flow run marked as crashed due to missing heartbeats."
    }
  ]
}
