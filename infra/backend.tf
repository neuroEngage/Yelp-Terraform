terraform {
  cloud {
    organization = "cdac-bda-group06"

    workspaces {
      name = "yelp-bigdata-workspace"
    }
  }
}
