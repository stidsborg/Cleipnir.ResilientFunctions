using System.Threading.Tasks;

namespace ConsoleApp.TravelAgency.ExternalServices;

public static class CarRentalService
{
    public static void Start()
    {
        MessageBroker.Subscribe(msg =>
        {
            if (msg is RentCar booking)
            {
                Task.Run(async () =>
                {
                    await Task.Delay(250);
                    await MessageBroker.Send(new CarRented(booking.BookingId));
                });
            }

            return Task.CompletedTask;
        }); 
    }
}