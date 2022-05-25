pub mod messages {

    pub struct JobInfo {
        pub name: String,
        pub id: String,
    }

    pub enum Notification {
        JobStarted(JobInfo),
        JobFinished(JobInfo),
    }
}

pub mod models {
    // TODO: rewrite this, see https://yew.rs/docs/concepts/components/children
    use yew::prelude::*;
    use super::messages::*;

    pub struct Job {
        pub name: String,
        pub status: String,
    }

    pub struct JobsList {
        pub jobs: Vec<Job>,
    }

    impl JobsList {
        pub fn new() -> Self {
            Self { jobs: Vec::new() }
        }
    }

    pub struct JobsView {
        pub jobs_list: JobsList,
    }

    impl Component for JobsList {

        type Message = Notification;
        type Properties = ();

        fn create(_ctx: &Context<Self>) -> Self {
            Self { jobs: Vec::new() }
        }

        fn update(&mut self, _ctx: &Context<Self>, msg: Self::Message) -> bool {
            match msg {
                Notification::JobStarted(j) => {
                    //self.value += 2;
                    // the value has changed so we need to
                    // re-render for it to appear on the page
                    true
                }
                _ => false,
            }
        }

        fn view(&self, ctx: &Context<Self>) -> Html {
            // This gives us a component's "`Scope`" which allows us to send messages, etc to the component.
            let link = ctx.link();
            html! {
                <div>
                </div>
            }
        }
    }

    impl From<JobsList> for Html {
        fn from(l: JobsList) -> Html {
            l.jobs.iter().map(|elem| &elem.name).collect::<Html>()
        }
    }

    impl Component for JobsView {
        type Message = Notification;
        type Properties = ();

        fn create(_ctx: &Context<Self>) -> Self {
            Self { jobs_list: JobsList::new() }
        }

        fn update(&mut self, _ctx: &Context<Self>, msg: Self::Message) -> bool {
            match msg {
                Notification::JobStarted(j) => {
                    //self.value += 2;
                    // the value has changed so we need to
                    // re-render for it to appear on the page
                    true
                }
                _ => false,
            }
        }

        fn view(&self, ctx: &Context<Self>) -> Html {
            // This gives us a component's "`Scope`" which allows us to send messages, etc to the component.
            let link = ctx.link();
            html! {
                <div>
                    &self.jobs_list
                </div>
            }
        }
    }
}
